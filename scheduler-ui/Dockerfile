# syntax=docker/dockerfile:1

# Build the static UI bundle. Node exists ONLY in this stage and ONLY at build
# time — at runtime the coordinator (not this image) serves the result, so the
# host never needs Node installed.
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install
COPY . .
RUN npm run build          # → /app/dist

# Export stage: a filesystem-only image containing just the built bundle. This
# lets the bundle be written to the host with
#   docker build --target export --output type=local,dest=dist .
# (see build-dist.sh) for the local host-process coordinator, OR be COPY'd into
# the coordinator image in prod:  COPY --from=<this image> /dist /opt/scheduler/ui
FROM scratch AS export
COPY --from=build /app/dist /
