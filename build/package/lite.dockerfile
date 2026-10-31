# This expects the hatchet-lite image to be built and available on the machine
# -------------------
ARG HATCHET_LITE_IMAGE
ARG HATCHET_ADMIN_IMAGE
ARG HATCHET_MIGRATE_IMAGE

# Stage 1: copy from the existing Go built image
FROM $HATCHET_LITE_IMAGE as lite-binary-base
FROM $HATCHET_ADMIN_IMAGE as admin-binary-base
FROM $HATCHET_MIGRATE_IMAGE as migrate-binary-base

# Stage 2: build the frontend
# TODO： base 镜像应该锁定版本和digest
FROM node:22-alpine as frontend-build

WORKDIR /app

# Optional npm registry override for networks that cannot reach
# registry.npmjs.org directly (empty = official registry). corepack fetches
# the pnpm tarball through COREPACK_NPM_REGISTRY; pnpm itself reads
# npm_config_registry.
ARG NPM_REGISTRY=""
COPY ./frontend/app/package.json ./frontend/app/pnpm-lock.yaml ./
RUN if [ -n "$NPM_REGISTRY" ]; then export COREPACK_NPM_REGISTRY="$NPM_REGISTRY" npm_config_registry="$NPM_REGISTRY"; fi && \
    corepack pnpm@10.16.1 --version
RUN if [ -n "$NPM_REGISTRY" ]; then export COREPACK_NPM_REGISTRY="$NPM_REGISTRY" npm_config_registry="$NPM_REGISTRY"; fi && \
    corepack pnpm@10.16.1 install --frozen-lockfile && corepack pnpm@10.16.1 store prune

COPY ./frontend/app ./
RUN npm run build

# Stage 3: deployment image from alpine
# TODO： base 镜像应该锁定版本和digest
FROM alpine AS deployment

# Optional Alpine mirror override (empty = official CDN).
ARG ALPINE_MIRROR=""

# install bash via apk
RUN if [ -n "$ALPINE_MIRROR" ]; then \
    sed -i -e "s|https://dl-cdn.alpinelinux.org|$ALPINE_MIRROR|g" -e "s|http://dl-cdn.alpinelinux.org|$ALPINE_MIRROR|g" /etc/apk/repositories; \
    fi && \
    apk update && apk add --no-cache bash openssl ca-certificates curl postgresql-client tzdata

COPY --from=lite-binary-base /hatchet/hatchet-lite ./hatchet-lite
COPY --from=admin-binary-base /hatchet/hatchet-admin ./hatchet-admin
COPY --from=migrate-binary-base /hatchet/hatchet-migrate ./hatchet-migrate
COPY --from=frontend-build /app/dist ./static-assets

# Copy entrypoint script
COPY ./hack/lite/start.sh ./entrypoint.sh

ENV LITE_STATIC_ASSET_DIR=/static-assets
ENV LITE_FRONTEND_BASE_PATH=/
ENV LITE_FRONTEND_PORT=8081
ENV LITE_RUNTIME_PORT=8888

# Make entrypoint script executable
RUN chmod +x ./entrypoint.sh

EXPOSE 8888 7070

# Run the entrypoint script
CMD ["./entrypoint.sh"]
