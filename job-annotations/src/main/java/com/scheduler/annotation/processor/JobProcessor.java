package com.scheduler.annotation.processor;

import com.scheduler.annotation.*;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.WildcardTypeName;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Annotation processor for {@link Job}-annotated classes. Runs at compile time and generates:
 * <ul>
 *   <li>{@code <ClassName>_Descriptor} — implements {@link com.scheduler.sdk.meta.JobDescriptor}</li>
 *   <li>{@code <ClassName>_Harness} — main entry point that decodes payload, runs lifecycle</li>
 * </ul>
 *
 * <p>Also registers all generated descriptors in
 * {@code META-INF/services/com.scheduler.sdk.meta.JobDescriptor} for ServiceLoader discovery.
 */
@SupportedAnnotationTypes("com.scheduler.annotation.Job")
@SupportedSourceVersion(SourceVersion.RELEASE_21)
public class JobProcessor extends AbstractProcessor {

    private static final ClassName JOB_DESCRIPTOR = ClassName.get("com.scheduler.sdk.meta", "JobDescriptor");
    private static final ClassName PARAM_DESCRIPTOR = ClassName.get("com.scheduler.sdk.meta", "ParamDescriptor");
    private static final ClassName RESOURCE_REQUIREMENTS = ClassName.get("com.scheduler.sdk.meta", "ResourceRequirements");
    private static final ClassName TASK_DESCRIPTOR = ClassName.get("com.scheduler.sdk.meta", "TaskDescriptor");
    private static final ClassName EXECUTION_PAYLOAD = ClassName.get("com.scheduler.sdk", "ExecutionPayload");
    private static final ClassName JOB_REPORTER = ClassName.get("com.scheduler.sdk", "JobReporter");
    private static final ClassName DEFAULT_JOB_CONTEXT = ClassName.get("com.scheduler.sdk", "DefaultJobContext");

    private final List<String> descriptorFqns = new ArrayList<>();

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        for (Element element : roundEnv.getElementsAnnotatedWith(Job.class)) {
            if (element.getKind() != ElementKind.CLASS) {
                error(element, "@Job can only be applied to classes");
                continue;
            }
            TypeElement jobClass = (TypeElement) element;

            if (!validate(jobClass)) {
                continue;
            }

            generateDescriptor(jobClass);
            generateHarness(jobClass);
        }

        if (roundEnv.processingOver() && !descriptorFqns.isEmpty()) {
            writeServiceFile();
        }

        return true;
    }

    // ── Validation ──────────────────────────────────────────────────────────

    private boolean validate(TypeElement jobClass) {
        boolean valid = true;

        List<ExecutableElement> taskMethods = getTaskMethods(jobClass);
        List<ExecutableElement> beforeMethods = getAnnotatedMethods(jobClass, BeforeJob.class);
        List<ExecutableElement> afterMethods = getAnnotatedMethods(jobClass, AfterJob.class);

        // Multiple @BeforeJob
        if (beforeMethods.size() > 1) {
            for (ExecutableElement m : beforeMethods) {
                error(m, "Only one @BeforeJob method is allowed per @Job class");
            }
            valid = false;
        }

        // Multiple @AfterJob
        if (afterMethods.size() > 1) {
            for (ExecutableElement m : afterMethods) {
                error(m, "Only one @AfterJob method is allowed per @Job class");
            }
            valid = false;
        }

        // @Task method returns non-void
        for (ExecutableElement method : taskMethods) {
            if (method.getReturnType().getKind() != TypeKind.VOID) {
                error(method, "@Task method must return void");
                valid = false;
            }
        }

        // @Task method parameter validation
        for (ExecutableElement method : taskMethods) {
            for (VariableElement param : method.getParameters()) {
                Param paramAnn = param.getAnnotation(Param.class);
                if (paramAnn != null && !isSupportedParamType(param.asType())) {
                    error(param, "@Param type must be one of: String, int, Integer, long, Long, double, Double, boolean, Boolean");
                    valid = false;
                }
            }
        }

        // Duplicate @Param names across constructor and task methods
        Set<String> paramNames = new HashSet<>();
        for (ExecutableElement constructor : getConstructors(jobClass)) {
            for (VariableElement param : constructor.getParameters()) {
                Param paramAnn = param.getAnnotation(Param.class);
                if (paramAnn != null) {
                    if (!paramNames.add(paramAnn.value())) {
                        error(param, "Duplicate @Param name: " + paramAnn.value());
                        valid = false;
                    }
                    if (!isSupportedParamType(param.asType())) {
                        error(param, "@Param type must be one of: String, int, Integer, long, Long, double, Double, boolean, Boolean");
                        valid = false;
                    }
                }
            }
        }

        // dependsOn validation
        Map<String, ExecutableElement> tasksByName = new LinkedHashMap<>();
        for (ExecutableElement method : taskMethods) {
            Task taskAnn = method.getAnnotation(Task.class);
            tasksByName.put(taskAnn.name(), method);
        }

        for (ExecutableElement method : taskMethods) {
            Task taskAnn = method.getAnnotation(Task.class);
            for (String dep : taskAnn.dependsOn()) {
                if (!tasksByName.containsKey(dep)) {
                    error(method, "@Task dependsOn references non-existent task: " + dep);
                    valid = false;
                }
            }
        }

        // Cycle detection (Kahn's algorithm)
        if (valid && !tasksByName.isEmpty()) {
            if (hasCycle(tasksByName)) {
                error(jobClass, "Cycle detected in @Task dependsOn graph");
                valid = false;
            }
        }

        return valid;
    }

    private boolean hasCycle(Map<String, ExecutableElement> tasksByName) {
        Map<String, Set<String>> adjacency = new HashMap<>();
        Map<String, Integer> inDegree = new HashMap<>();

        for (String name : tasksByName.keySet()) {
            adjacency.put(name, new HashSet<>());
            inDegree.put(name, 0);
        }

        for (Map.Entry<String, ExecutableElement> entry : tasksByName.entrySet()) {
            Task taskAnn = entry.getValue().getAnnotation(Task.class);
            for (String dep : taskAnn.dependsOn()) {
                if (adjacency.containsKey(dep)) {
                    adjacency.get(dep).add(entry.getKey());
                    inDegree.merge(entry.getKey(), 1, Integer::sum);
                }
            }
        }

        List<String> queue = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                queue.add(entry.getKey());
            }
        }

        int processed = 0;
        while (!queue.isEmpty()) {
            String current = queue.removeFirst();
            processed++;
            for (String neighbor : adjacency.get(current)) {
                int newDegree = inDegree.get(neighbor) - 1;
                inDegree.put(neighbor, newDegree);
                if (newDegree == 0) {
                    queue.add(neighbor);
                }
            }
        }

        return processed != tasksByName.size();
    }

    private boolean isSupportedParamType(TypeMirror type) {
        String name = type.toString();
        return name.equals("java.lang.String")
                || name.equals("int") || name.equals("java.lang.Integer")
                || name.equals("long") || name.equals("java.lang.Long")
                || name.equals("double") || name.equals("java.lang.Double")
                || name.equals("boolean") || name.equals("java.lang.Boolean");
    }

    // ── _Descriptor generation ──────────────────────────────────────────────

    private void generateDescriptor(TypeElement jobClass) {
        Job jobAnn = jobClass.getAnnotation(Job.class);
        PackageElement pkg = (PackageElement) jobClass.getEnclosingElement();
        String packageName = pkg.getQualifiedName().toString();
        String className = jobClass.getSimpleName().toString();
        String descriptorName = className + "_Descriptor";

        descriptorFqns.add(packageName + "." + descriptorName);

        ResourceProfile res = jobAnn.resource();
        List<ExecutableElement> taskMethods = getTaskMethods(jobClass);
        List<ParamInfo> paramInfos = getConstructorParams(jobClass);
        ClassName jobClassName = ClassName.get(packageName, className);

        MethodSpec id = overrideMethod("id", String.class)
                .addStatement("return $S", jobAnn.id()).build();

        MethodSpec description = overrideMethod("description", String.class)
                .addStatement("return $S", jobAnn.description()).build();

        MethodSpec timeoutSeconds = overrideMethod("timeoutSeconds", int.class)
                .addStatement("return $L", jobAnn.timeoutSeconds()).build();

        MethodSpec maxRetries = overrideMethod("maxRetries", int.class)
                .addStatement("return $L", jobAnn.maxRetries()).build();

        MethodSpec resources = overrideMethod("resources", RESOURCE_REQUIREMENTS)
                .addStatement("return new $T($L, $L, $L)",
                        RESOURCE_REQUIREMENTS, res.minMemoryMb(), res.cpuCores(), setOfStrings(res.labels()))
                .build();

        MethodSpec parameters = overrideMethod("parameters",
                ParameterizedTypeName.get(ClassName.get(List.class), PARAM_DESCRIPTOR))
                .addStatement("return $L", paramDescriptorList(paramInfos)).build();

        MethodSpec tasks = overrideMethod("tasks",
                ParameterizedTypeName.get(ClassName.get(List.class), TASK_DESCRIPTOR))
                .addStatement("return $L", taskDescriptorList(taskMethods)).build();

        MethodSpec jobClassMethod = overrideMethod("jobClass",
                ParameterizedTypeName.get(ClassName.get(Class.class), WildcardTypeName.subtypeOf(Object.class)))
                .addStatement("return $T.class", jobClassName).build();

        TypeSpec descriptor = TypeSpec.classBuilder(descriptorName)
                .addJavadoc("Generated by JobProcessor for {@link $L}.", className)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addSuperinterface(JOB_DESCRIPTOR)
                .addMethod(id)
                .addMethod(description)
                .addMethod(timeoutSeconds)
                .addMethod(maxRetries)
                .addMethod(resources)
                .addMethod(parameters)
                .addMethod(tasks)
                .addMethod(jobClassMethod)
                .build();

        try {
            JavaFile.builder(packageName, descriptor).build().writeTo(processingEnv.getFiler());
        } catch (IOException e) {
            error(jobClass, "Failed to generate " + descriptorName + ": " + e.getMessage());
        }
    }

    // ── _Harness generation ─────────────────────────────────────────────────

    private void generateHarness(TypeElement jobClass) {
        PackageElement pkg = (PackageElement) jobClass.getEnclosingElement();
        String packageName = pkg.getQualifiedName().toString();
        String className = jobClass.getSimpleName().toString();
        String harnessName = className + "_Harness";

        List<ExecutableElement> taskMethods = topologicalSort(getTaskMethods(jobClass));
        List<ParamInfo> constructorParams = getConstructorParams(jobClass);
        ExecutableElement beforeMethod = getAnnotatedMethods(jobClass, BeforeJob.class).stream().findFirst().orElse(null);
        ExecutableElement afterMethod = getAnnotatedMethods(jobClass, AfterJob.class).stream().findFirst().orElse(null);

        ClassName jobClassName = ClassName.get(packageName, className);

        CodeBlock.Builder body = CodeBlock.builder();
        body.addStatement("$T payload = $T.decode(args[0])", EXECUTION_PAYLOAD, EXECUTION_PAYLOAD);
        body.addStatement("$T reporter = $T.connect(payload.workerAgentUrl(), payload.jobId())", JOB_REPORTER, JOB_REPORTER);
        body.addStatement("$T ctx = new $T(reporter)", DEFAULT_JOB_CONTEXT, DEFAULT_JOB_CONTEXT);
        body.add("\n");
        body.addStatement("$T job = new $T($L)", jobClassName, jobClassName, constructorArgs(constructorParams));
        body.add("\n");

        body.beginControlFlow("try");

        if (beforeMethod != null) {
            body.addStatement("job.$L()", beforeMethod.getSimpleName());
        }

        for (int i = 0; i < taskMethods.size(); i++) {
            body.add("\n");
            body.add(taskBlock(i, taskMethods.get(i)));
        }

        body.add("\n");
        if (afterMethod != null) {
            body.addStatement("job.$L()", afterMethod.getSimpleName());
        }

        body.nextControlFlow("catch ($T e)", Exception.class);
        if (afterMethod != null) {
            body.beginControlFlow("try");
            body.addStatement("job.$L()", afterMethod.getSimpleName());
            body.nextControlFlow("catch ($T suppressed)", Exception.class);
            body.addStatement("e.addSuppressed(suppressed)");
            body.endControlFlow();
        }
        body.addStatement("$T.exit(1)", System.class);
        body.endControlFlow();

        MethodSpec main = MethodSpec.methodBuilder("main")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                .returns(void.class)
                .addParameter(String[].class, "args")
                .addCode(body.build())
                .build();

        TypeSpec harness = TypeSpec.classBuilder(harnessName)
                .addJavadoc("Generated entry point for {@link $L}.", className)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(main)
                .build();

        try {
            JavaFile.builder(packageName, harness).build().writeTo(processingEnv.getFiler());
        } catch (IOException e) {
            error(jobClass, "Failed to generate " + harnessName + ": " + e.getMessage());
        }
    }

    // ── ServiceLoader registration ──────────────────────────────────────────

    private void writeServiceFile() {
        try {
            FileObject file = processingEnv.getFiler().createResource(
                    StandardLocation.CLASS_OUTPUT, "",
                    "META-INF/services/com.scheduler.sdk.meta.JobDescriptor");
            try (Writer w = file.openWriter()) {
                for (String fqn : descriptorFqns) {
                    w.write(fqn);
                    w.write("\n");
                }
            }
        } catch (IOException e) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                    "Failed to write ServiceLoader file: " + e.getMessage());
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static MethodSpec.Builder overrideMethod(String name, java.lang.reflect.Type returnType) {
        return MethodSpec.methodBuilder(name)
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(returnType);
    }

    private static MethodSpec.Builder overrideMethod(String name, com.squareup.javapoet.TypeName returnType) {
        return MethodSpec.methodBuilder(name)
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(returnType);
    }

    // ── CodeBlock builders ─────────────────────────────────────────────────

    /** Produces {@code Set.of("a", "b")}. */
    private static CodeBlock setOfStrings(String[] values) {
        CodeBlock.Builder b = CodeBlock.builder().add("$T.of(", Set.class);
        for (int i = 0; i < values.length; i++) {
            if (i > 0) b.add(", ");
            b.add("$S", values[i]);
        }
        return b.add(")").build();
    }

    /** Produces {@code List.of("a", "b")}. */
    private static CodeBlock listOfStrings(String[] values) {
        CodeBlock.Builder b = CodeBlock.builder().add("$T.of(", List.class);
        for (int i = 0; i < values.length; i++) {
            if (i > 0) b.add(", ");
            b.add("$S", values[i]);
        }
        return b.add(")").build();
    }

    /** Produces {@code List.of(new ParamDescriptor("name", String.class, true, null), ...)}. */
    private CodeBlock paramDescriptorList(List<ParamInfo> paramInfos) {
        CodeBlock.Builder b = CodeBlock.builder().add("$T.of(", List.class);
        for (int i = 0; i < paramInfos.size(); i++) {
            ParamInfo p = paramInfos.get(i);
            if (i > 0) b.add(", ");
            if (p.defaultValue.isEmpty()) {
                b.add("new $T($S, $L, $L, null)", PARAM_DESCRIPTOR, p.name, p.typeClass, p.required);
            } else {
                b.add("new $T($S, $L, $L, $S)", PARAM_DESCRIPTOR, p.name, p.typeClass, p.required, p.defaultValue);
            }
        }
        return b.add(")").build();
    }

    /** Produces {@code List.of(new TaskDescriptor("extract", 1, List.of(), true), ...)}. */
    private CodeBlock taskDescriptorList(List<ExecutableElement> taskMethods) {
        CodeBlock.Builder b = CodeBlock.builder().add("$T.of(", List.class);
        for (int i = 0; i < taskMethods.size(); i++) {
            ExecutableElement method = taskMethods.get(i);
            Task taskAnn = method.getAnnotation(Task.class);
            if (i > 0) b.add(", ");
            b.add("new $T($S, $L, $L, $L)",
                    TASK_DESCRIPTOR, taskAnn.name(), taskAnn.order(),
                    listOfStrings(taskAnn.dependsOn()), taskAnn.critical());
        }
        return b.add(")").build();
    }

    /** Produces {@code payload.param("region", String.class), payload.param("batchSize", int.class)}. */
    private static CodeBlock constructorArgs(List<ParamInfo> params) {
        CodeBlock.Builder b = CodeBlock.builder();
        for (int i = 0; i < params.size(); i++) {
            ParamInfo p = params.get(i);
            if (i > 0) b.add(", ");
            b.add("payload.param($S, $L)", p.name, p.typeClass);
        }
        return b.build();
    }

    /** Produces the try/catch block for a single task: report started, invoke, report completed/failed. */
    private CodeBlock taskBlock(int index, ExecutableElement method) {
        Task taskAnn = method.getAnnotation(Task.class);
        String taskName = taskAnn.name();

        CodeBlock.Builder b = CodeBlock.builder();
        b.addStatement("reporter.taskStarted($L, $S)", index, taskName);
        b.beginControlFlow("try");
        b.addStatement("job.$L($L)", method.getSimpleName(), taskMethodArgs(method));
        b.addStatement("reporter.taskCompleted($L, $S)", index, taskName);
        b.nextControlFlow("catch ($T e)", Exception.class);
        b.addStatement("reporter.taskFailed($L, $S, e.getMessage())", index, taskName);
        b.addStatement("throw e");
        b.endControlFlow();
        return b.build();
    }

    /** Produces argument list for a @Task method: {@code ctx} for @Context, {@code payload.param(...)} for @Param. */
    private CodeBlock taskMethodArgs(ExecutableElement method) {
        CodeBlock.Builder b = CodeBlock.builder();
        List<? extends VariableElement> params = method.getParameters();
        for (int i = 0; i < params.size(); i++) {
            VariableElement param = params.get(i);
            if (i > 0) b.add(", ");
            if (param.getAnnotation(Context.class) != null) {
                b.add("ctx");
            } else {
                Param paramAnn = param.getAnnotation(Param.class);
                if (paramAnn != null) {
                    b.add("payload.param($S, $L)", paramAnn.value(), toClassLiteral(param.asType()));
                }
            }
        }
        return b.build();
    }

    private List<ExecutableElement> getTaskMethods(TypeElement jobClass) {
        List<ExecutableElement> result = new ArrayList<>();
        for (Element enclosed : jobClass.getEnclosedElements()) {
            if (enclosed.getKind() == ElementKind.METHOD && enclosed.getAnnotation(Task.class) != null) {
                result.add((ExecutableElement) enclosed);
            }
        }
        // Sort by order, then by source position (natural iteration order)
        result.sort((a, b) -> {
            int orderA = a.getAnnotation(Task.class).order();
            int orderB = b.getAnnotation(Task.class).order();
            return Integer.compare(orderA, orderB);
        });
        return result;
    }

    private <A extends java.lang.annotation.Annotation> List<ExecutableElement> getAnnotatedMethods(
            TypeElement jobClass, Class<A> annotationType) {
        List<ExecutableElement> result = new ArrayList<>();
        for (Element enclosed : jobClass.getEnclosedElements()) {
            if (enclosed.getKind() == ElementKind.METHOD && enclosed.getAnnotation(annotationType) != null) {
                result.add((ExecutableElement) enclosed);
            }
        }
        return result;
    }

    private List<ExecutableElement> getConstructors(TypeElement jobClass) {
        List<ExecutableElement> result = new ArrayList<>();
        for (Element enclosed : jobClass.getEnclosedElements()) {
            if (enclosed.getKind() == ElementKind.CONSTRUCTOR) {
                result.add((ExecutableElement) enclosed);
            }
        }
        return result;
    }

    private List<ParamInfo> getConstructorParams(TypeElement jobClass) {
        List<ParamInfo> result = new ArrayList<>();
        for (ExecutableElement constructor : getConstructors(jobClass)) {
            for (VariableElement param : constructor.getParameters()) {
                Param paramAnn = param.getAnnotation(Param.class);
                if (paramAnn != null) {
                    String typeClass = toClassLiteral(param.asType());
                    boolean required = paramAnn.defaultValue().isEmpty();
                    result.add(new ParamInfo(paramAnn.value(), typeClass, required, paramAnn.defaultValue()));
                }
            }
        }
        return result;
    }

    private List<ExecutableElement> topologicalSort(List<ExecutableElement> taskMethods) {
        Map<String, ExecutableElement> byName = new LinkedHashMap<>();
        for (ExecutableElement method : taskMethods) {
            byName.put(method.getAnnotation(Task.class).name(), method);
        }

        Map<String, Set<String>> adjacency = new HashMap<>();
        Map<String, Integer> inDegree = new HashMap<>();
        for (String name : byName.keySet()) {
            adjacency.put(name, new HashSet<>());
            inDegree.put(name, 0);
        }
        for (Map.Entry<String, ExecutableElement> entry : byName.entrySet()) {
            Task taskAnn = entry.getValue().getAnnotation(Task.class);
            for (String dep : taskAnn.dependsOn()) {
                if (adjacency.containsKey(dep)) {
                    adjacency.get(dep).add(entry.getKey());
                    inDegree.merge(entry.getKey(), 1, Integer::sum);
                }
            }
        }

        // Use order as tiebreaker within same dependency level
        List<String> queue = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                queue.add(entry.getKey());
            }
        }
        queue.sort((a, b) -> {
            int orderA = byName.get(a).getAnnotation(Task.class).order();
            int orderB = byName.get(b).getAnnotation(Task.class).order();
            return Integer.compare(orderA, orderB);
        });

        List<ExecutableElement> sorted = new ArrayList<>();
        while (!queue.isEmpty()) {
            String current = queue.removeFirst();
            sorted.add(byName.get(current));

            List<String> nextBatch = new ArrayList<>();
            for (String neighbor : adjacency.get(current)) {
                int newDegree = inDegree.get(neighbor) - 1;
                inDegree.put(neighbor, newDegree);
                if (newDegree == 0) {
                    nextBatch.add(neighbor);
                }
            }
            nextBatch.sort((a, b) -> {
                int orderA = byName.get(a).getAnnotation(Task.class).order();
                int orderB = byName.get(b).getAnnotation(Task.class).order();
                return Integer.compare(orderA, orderB);
            });
            queue.addAll(nextBatch);
        }

        return sorted;
    }

    private String toClassLiteral(TypeMirror type) {
        String name = type.toString();
        return switch (name) {
            case "int" -> "int.class";
            case "long" -> "long.class";
            case "double" -> "double.class";
            case "boolean" -> "boolean.class";
            case "java.lang.String" -> "String.class";
            case "java.lang.Integer" -> "Integer.class";
            case "java.lang.Long" -> "Long.class";
            case "java.lang.Double" -> "Double.class";
            case "java.lang.Boolean" -> "Boolean.class";
            default -> name + ".class";
        };
    }

    private void error(Element element, String message) {
        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, message, element);
    }

    private record ParamInfo(String name, String typeClass, boolean required, String defaultValue) {}
}
