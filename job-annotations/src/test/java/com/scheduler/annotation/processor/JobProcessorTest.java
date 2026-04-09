package com.scheduler.annotation.processor;

import com.scheduler.sdk.meta.JobDescriptor;
import com.scheduler.sdk.meta.ParamDescriptor;
import com.scheduler.sdk.meta.TaskDescriptor;
import org.junit.jupiter.api.Test;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link JobProcessor}, the annotation processor that generates {@code _Descriptor}
 * and {@code _Harness} classes from {@code @Job}-annotated sources.
 *
 * <p>Uses {@code javax.tools.JavaCompiler} to compile inline Java sources in-process with
 * {@code JobProcessor} registered as the processor. No external compile-testing library needed —
 * just the JDK's built-in compiler API.
 *
 * <h3>How the annotation system works</h3>
 *
 * <p>Job authors annotate a plain Java class:
 * <ul>
 *   <li>{@code @Job(id, description, timeoutSeconds, maxRetries, resource)} — marks the class</li>
 *   <li>{@code @Task(name, order, dependsOn, critical)} — marks methods as executable tasks</li>
 *   <li>{@code @Param("name")} — on constructor parameters, declares job inputs</li>
 *   <li>{@code @Context} — on task method parameters, injects the {@code JobContext}</li>
 *   <li>{@code @BeforeJob} / {@code @AfterJob} — lifecycle hooks (at most one each)</li>
 *   <li>{@code @ResourceProfile(minMemoryMb, cpuCores, labels)} — nested in @Job for resource hints</li>
 * </ul>
 *
 * <p>At compile time, {@code JobProcessor} validates the annotations and generates two classes:
 * <ul>
 *   <li>{@code <ClassName>_Descriptor} — implements {@code JobDescriptor}; exposes metadata
 *       (id, params, tasks, resources) so the coordinator can inspect a job without instantiating it</li>
 *   <li>{@code <ClassName>_Harness} — a {@code main()} entry point that decodes the payload,
 *       instantiates the job, runs lifecycle hooks, and executes tasks in topological order
 *       while reporting status back to the worker agent</li>
 * </ul>
 */
class JobProcessorTest {

    // ── Happy-path tests ────────────────────────────────────────────────────

    /**
     * Minimal valid job: one @Job class with a single @Task method.
     *
     * This is the simplest possible annotation-based job. The processor should:
     * 1. Accept it without errors
     * 2. Generate a _Descriptor implementing JobDescriptor
     * 3. Generate a _Harness with a main() entry point
     */
    @Test
    void validJob() throws Exception {
        CompilationResult result = compile("test.SimpleJob", """
                package test;
                import com.scheduler.annotation.*;

                @Job(id = "simple-job")
                public class SimpleJob {
                    @Task(name = "run")
                    public void run() {}
                }
                """);

        assertTrue(result.success, result.errorMessages());
        assertTrue(result.hasGeneratedSource("SimpleJob_Descriptor"));
        assertTrue(result.hasGeneratedSource("SimpleJob_Harness"));
    }

    /**
     * @Job metadata propagation: all @Job attributes and @ResourceProfile values
     * should appear in the generated _Descriptor's method return values.
     *
     * The descriptor is a compile-time-generated implementation of JobDescriptor that
     * exposes job metadata for the coordinator and worker to read without instantiating
     * the job class. Each @Job attribute maps to a descriptor method:
     *   id()             → @Job(id = ...)
     *   description()    → @Job(description = ...)
     *   timeoutSeconds() → @Job(timeoutSeconds = ...)
     *   maxRetries()     → @Job(maxRetries = ...)
     *   resources()      → @Job(resource = @ResourceProfile(...))
     */
    @Test
    void descriptorMetadata() throws Exception {
        CompilationResult result = compile("test.EtlJob", """
                package test;
                import com.scheduler.annotation.*;

                @Job(id = "etl-pipeline", description = "Daily ETL",
                     timeoutSeconds = 3600, maxRetries = 3,
                     resource = @ResourceProfile(minMemoryMb = 2048, cpuCores = 4, labels = {"gpu", "high-mem"}))
                public class EtlJob {
                    @Task(name = "extract")
                    public void extract() {}
                }
                """);

        assertTrue(result.success, result.errorMessages());
        JobDescriptor descriptor = result.loadDescriptor("test.EtlJob_Descriptor");

        assertEquals("etl-pipeline", descriptor.id());
        assertEquals("Daily ETL", descriptor.description());
        assertEquals(3600, descriptor.timeoutSeconds());
        assertEquals(3, descriptor.maxRetries());
        assertEquals(2048, descriptor.resources().memoryMb());
        assertEquals(4, descriptor.resources().cpuCores());
        assertEquals(Set.of("gpu", "high-mem"), descriptor.resources().labels());
    }

    /**
     * @Param on constructor parameters → ParamDescriptor entries in the _Descriptor.
     *
     * Constructor @Param annotations declare what inputs a job accepts. The processor
     * extracts each param's name, type, and default value to build ParamDescriptor objects.
     * A param with no defaultValue is required; one with a defaultValue is optional.
     *
     * At runtime, the _Harness calls payload.param("name", Type.class) to extract each
     * value from the ExecutionPayload.
     */
    @Test
    void descriptorParams() throws Exception {
        CompilationResult result = compile("test.ParamJob", """
                package test;
                import com.scheduler.annotation.*;

                @Job(id = "param-job")
                public class ParamJob {
                    public ParamJob(@Param("region") String region,
                                    @Param(value = "batchSize", defaultValue = "1000") int batchSize) {}
                    @Task(name = "run")
                    public void run() {}
                }
                """);

        assertTrue(result.success, result.errorMessages());
        JobDescriptor descriptor = result.loadDescriptor("test.ParamJob_Descriptor");
        List<ParamDescriptor> params = descriptor.parameters();

        assertEquals(2, params.size());

        // Required param: no default value → required=true, defaultValue=null
        ParamDescriptor region = params.get(0);
        assertEquals("region", region.name());
        assertEquals(String.class, region.type());
        assertTrue(region.required());
        assertNull(region.defaultValue());

        // Optional param: has default → required=false, defaultValue="1000"
        ParamDescriptor batchSize = params.get(1);
        assertEquals("batchSize", batchSize.name());
        assertEquals(int.class, batchSize.type());
        assertFalse(batchSize.required());
        assertEquals("1000", batchSize.defaultValue());
    }

    /**
     * @Task dependency chain → TaskDescriptor list with correct order and dependencies.
     *
     * Tasks form a DAG (directed acyclic graph) via the dependsOn attribute. The processor
     * topologically sorts them so the _Harness executes them in dependency order. The
     * _Descriptor exposes the task graph so the coordinator can display it or validate it.
     *
     * The 'critical' flag on a task means its failure should abort the entire job.
     */
    @Test
    void descriptorTasks() throws Exception {
        CompilationResult result = compile("test.PipelineJob", """
                package test;
                import com.scheduler.annotation.*;

                @Job(id = "pipeline-job")
                public class PipelineJob {
                    @Task(name = "extract", order = 1)
                    public void extract() {}

                    @Task(name = "transform", order = 2, dependsOn = "extract")
                    public void transform() {}

                    @Task(name = "load", order = 3, dependsOn = "transform", critical = true)
                    public void load() {}
                }
                """);

        assertTrue(result.success, result.errorMessages());
        JobDescriptor descriptor = result.loadDescriptor("test.PipelineJob_Descriptor");
        List<TaskDescriptor> tasks = descriptor.tasks();

        assertEquals(3, tasks.size());

        TaskDescriptor extract = tasks.get(0);
        assertEquals("extract", extract.name());
        assertEquals(1, extract.order());
        assertEquals(List.of(), extract.dependsOn());
        assertFalse(extract.critical());

        TaskDescriptor transform = tasks.get(1);
        assertEquals("transform", transform.name());
        assertEquals(2, transform.order());
        assertEquals(List.of("extract"), transform.dependsOn());
        assertFalse(transform.critical());

        TaskDescriptor load = tasks.get(2);
        assertEquals("load", load.name());
        assertEquals(3, load.order());
        assertEquals(List.of("transform"), load.dependsOn());
        assertTrue(load.critical());
    }

    /**
     * @BeforeJob and @AfterJob lifecycle hooks → harness calls them at the right time.
     *
     * @BeforeJob runs once before any @Task methods. @AfterJob runs after all tasks complete,
     * and is also called on a best-effort basis in the catch block if a task fails (for cleanup).
     *
     * The generated _Harness wraps execution in a try/catch:
     *   try { beforeJob(); tasks...; afterJob(); }
     *   catch { afterJob(); System.exit(1); }
     */
    @Test
    void harnessLifecycle() throws IOException {
        CompilationResult result = compile("test.LifecycleJob", """
                package test;
                import com.scheduler.annotation.*;

                @Job(id = "lifecycle-job")
                public class LifecycleJob {
                    @BeforeJob
                    public void setup() {}

                    @Task(name = "run")
                    public void run() {}

                    @AfterJob
                    public void teardown() {}
                }
                """);

        assertTrue(result.success, result.errorMessages());
        String harness = result.generatedSource("LifecycleJob_Harness");

        // @BeforeJob method is called before tasks
        assertTrue(harness.contains("job.setup()"), "calls @BeforeJob method");

        // @AfterJob method is called after tasks (happy path and error path)
        assertTrue(harness.contains("job.teardown()"), "calls @AfterJob method");

        // Harness has the standard structure: payload decode, reporter, context
        assertTrue(harness.contains("ExecutionPayload.decode"), "decodes payload");
        assertTrue(harness.contains("JobReporter.connect"), "connects reporter");
    }

    // ── Error tests ─────────────────────────────────────────────────────────

    /**
     * At most one @BeforeJob method is allowed per @Job class.
     * Multiple @BeforeJob methods would be ambiguous — which runs first?
     */
    @Test
    void multipleBeforeJob() throws IOException {
        CompilationResult result = compile("test.BadJob", """
                package test;
                import com.scheduler.annotation.*;

                @Job(id = "bad-job")
                public class BadJob {
                    @BeforeJob public void setup1() {}
                    @BeforeJob public void setup2() {}
                    @Task(name = "run")
                    public void run() {}
                }
                """);

        assertFalse(result.success);
        assertTrue(result.hasError("Only one @BeforeJob method is allowed"),
                result.errorMessages());
    }

    /**
     * At most one @AfterJob method is allowed per @Job class.
     * Same rationale as @BeforeJob — multiple cleanup hooks would be ambiguous.
     */
    @Test
    void multipleAfterJob() throws IOException {
        CompilationResult result = compile("test.BadJob", """
                package test;
                import com.scheduler.annotation.*;

                @Job(id = "bad-job")
                public class BadJob {
                    @AfterJob public void cleanup1() {}
                    @AfterJob public void cleanup2() {}
                    @Task(name = "run")
                    public void run() {}
                }
                """);

        assertFalse(result.success);
        assertTrue(result.hasError("Only one @AfterJob method is allowed"),
                result.errorMessages());
    }

    /**
     * @Task methods must return void. The harness calls them for their side effects
     * (reading data, writing data, calling APIs) — return values would be silently ignored.
     */
    @Test
    void nonVoidTask() throws IOException {
        CompilationResult result = compile("test.BadJob", """
                package test;
                import com.scheduler.annotation.*;

                @Job(id = "bad-job")
                public class BadJob {
                    @Task(name = "compute")
                    public int compute() { return 42; }
                }
                """);

        assertFalse(result.success);
        assertTrue(result.hasError("@Task method must return void"),
                result.errorMessages());
    }

    /**
     * @Param types are restricted to primitives and their wrappers plus String.
     * These are the types that ExecutionPayload.param() can coerce from the string-based
     * parameter map. Complex types like List<String> are not supported.
     */
    @Test
    void unsupportedParamType() throws IOException {
        CompilationResult result = compile("test.BadJob", """
                package test;
                import com.scheduler.annotation.*;
                import java.util.List;

                @Job(id = "bad-job")
                public class BadJob {
                    @Task(name = "run")
                    public void run(@Param("items") List<String> items) {}
                }
                """);

        assertFalse(result.success);
        assertTrue(result.hasError("@Param type must be one of"),
                result.errorMessages());
    }

    /**
     * Each @Param name must be unique within a job's constructor. Duplicate names would
     * cause the same payload key to be extracted twice, with no way to distinguish them.
     */
    @Test
    void duplicateParam() throws IOException {
        CompilationResult result = compile("test.BadJob", """
                package test;
                import com.scheduler.annotation.*;

                @Job(id = "bad-job")
                public class BadJob {
                    public BadJob(@Param("region") String region,
                                  @Param("region") String region2) {}
                    @Task(name = "run")
                    public void run() {}
                }
                """);

        assertFalse(result.success);
        assertTrue(result.hasError("Duplicate @Param name: region"),
                result.errorMessages());
    }

    /**
     * dependsOn must reference an existing @Task name. A typo or renamed task should
     * fail at compile time rather than silently producing a broken execution graph.
     */
    @Test
    void missingDependency() throws IOException {
        CompilationResult result = compile("test.BadJob", """
                package test;
                import com.scheduler.annotation.*;

                @Job(id = "bad-job")
                public class BadJob {
                    @Task(name = "load", dependsOn = "nonexistent")
                    public void load() {}
                }
                """);

        assertFalse(result.success);
        assertTrue(result.hasError("dependsOn references non-existent task"),
                result.errorMessages());
    }

    /**
     * Circular dependencies (A → B → A) would cause an infinite execution loop.
     * The processor uses Kahn's algorithm to detect cycles at compile time.
     */
    @Test
    void cyclicDependency() throws IOException {
        CompilationResult result = compile("test.BadJob", """
                package test;
                import com.scheduler.annotation.*;

                @Job(id = "bad-job")
                public class BadJob {
                    @Task(name = "a", dependsOn = "b")
                    public void a() {}

                    @Task(name = "b", dependsOn = "a")
                    public void b() {}
                }
                """);

        assertFalse(result.success);
        assertTrue(result.hasError("Cycle detected"),
                result.errorMessages());
    }

    // ── Test infrastructure ─────────────────────────────────────────────────

    /**
     * Compiles an inline Java source with {@link JobProcessor} registered as the
     * annotation processor. Uses the JDK's built-in {@code javax.tools.JavaCompiler}.
     *
     * @param className fully qualified class name (e.g. "test.SimpleJob")
     * @param source    the complete Java source code
     * @return result with success flag, diagnostics, and access to generated sources/classes
     */
    private CompilationResult compile(String className, String source) throws IOException {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();

        Path classOutput = Files.createTempDirectory("job-processor-classes");
        Path sourceOutput = Files.createTempDirectory("job-processor-sources");

        List<String> options = List.of(
                "-classpath", System.getProperty("java.class.path"),
                "-d", classOutput.toString(),
                "-s", sourceOutput.toString()
        );

        JavaFileObject sourceFile = new SimpleJavaFileObject(
                URI.create("string:///" + className.replace('.', '/') + JavaFileObject.Kind.SOURCE.extension),
                JavaFileObject.Kind.SOURCE
        ) {
            @Override
            public CharSequence getCharContent(boolean ignoreEncodingErrors) {
                return source;
            }
        };

        JavaCompiler.CompilationTask task = compiler.getTask(
                null, null, diagnostics, options, null, List.of(sourceFile));
        task.setProcessors(List.of(new JobProcessor()));

        boolean success = task.call();
        return new CompilationResult(success, diagnostics.getDiagnostics(), sourceOutput, classOutput);
    }

    /**
     * Captures the outcome of a compilation: success/failure, error diagnostics,
     * and the directories where generated source and class files were written.
     */
    private record CompilationResult(
            boolean success,
            List<Diagnostic<? extends JavaFileObject>> diagnostics,
            Path sourceOutput,
            Path classOutput
    ) {
        /** True if any error diagnostic contains the given fragment. */
        boolean hasError(String fragment) {
            return diagnostics.stream()
                    .filter(d -> d.getKind() == Diagnostic.Kind.ERROR)
                    .anyMatch(d -> d.getMessage(Locale.ROOT).contains(fragment));
        }

        /** True if a generated source file with this simple name exists. */
        boolean hasGeneratedSource(String simpleName) {
            try (var stream = Files.walk(sourceOutput)) {
                return stream.anyMatch(p -> p.getFileName().toString().equals(simpleName + ".java"));
            } catch (IOException e) {
                return false;
            }
        }

        /** Reads the content of a generated source file by simple name. */
        String generatedSource(String simpleName) throws IOException {
            try (var stream = Files.walk(sourceOutput)) {
                Path found = stream
                        .filter(p -> p.getFileName().toString().equals(simpleName + ".java"))
                        .findFirst()
                        .orElseThrow(() -> new AssertionError(
                                "Generated source not found: " + simpleName + ".java"));
                return Files.readString(found);
            }
        }

        /**
         * Loads and instantiates a generated _Descriptor class from the compiled output.
         * Uses a URLClassLoader pointing at the classOutput directory so the generated
         * .class files can be found. The parent classloader provides the SDK types
         * (JobDescriptor, ParamDescriptor, etc.) that the generated class implements.
         */
        JobDescriptor loadDescriptor(String fqn) throws Exception {
            URL[] urls = {classOutput.toUri().toURL()};
            try (URLClassLoader loader = new URLClassLoader(urls, JobDescriptor.class.getClassLoader())) {
                Class<?> clazz = loader.loadClass(fqn);
                return (JobDescriptor) clazz.getDeclaredConstructor().newInstance();
            }
        }

        /** All error messages joined, for use as assertion failure messages. */
        String errorMessages() {
            return diagnostics.stream()
                    .filter(d -> d.getKind() == Diagnostic.Kind.ERROR)
                    .map(d -> d.getMessage(Locale.ROOT))
                    .reduce((a, b) -> a + "\n" + b)
                    .orElse("(no errors)");
        }
    }
}
