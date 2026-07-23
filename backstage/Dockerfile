# Multi-stage build, entirely self-contained (no host yarn/node, matching
# every other service in this repo — see the root Dockerfile's ARG SERVICE
# pattern). Builds both packages/backend and packages/app; the backend's
# @backstage/plugin-app-backend serves the app's static bundle on the same
# port in production, so this ends up a single container / single port
# (7007), same "one container, one port" shape as every Go service here.
#
# Build context is the REPO ROOT, not backstage/ — this Dockerfile also
# needs the sibling catalog-info.yaml/mkdocs.yml/docs/ it catalogs, which a
# ./backstage-scoped context can't reach (see docker-compose.yml's
# `backstage` service and the root .dockerignore).
#
# Reference: https://backstage.io/docs/deployment/docker#multi-stage-build

FROM node:22-bookworm-slim AS build
WORKDIR /app

# python3/g++/build-essential: native deps (better-sqlite3, isolated-vm
# used by the scaffolder plugin). libsqlite3-dev: better-sqlite3's runtime
# lib, needed in both stages since it's a native addon, not pure JS.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && \
    apt-get install -y --no-install-recommends python3 g++ build-essential libsqlite3-dev && \
    rm -rf /var/lib/apt/lists/*

COPY backstage/ ./
RUN yarn install --immutable
RUN yarn tsc
RUN yarn build:backend
RUN yarn workspace app build

# Catalog source, copied to the image root (not /app) so it sits at the
# same relative depth from packages/backend as it does from
# backstage/packages/backend in a local monorepo checkout — app-config.yaml's
# `../../../catalog-info.yaml` catalog location target then resolves
# identically in both local dev (`yarn start`) and this container, no
# environment-specific path override needed.
COPY catalog-info.yaml mkdocs.yml /
COPY docs/ /docs/

FROM node:22-bookworm-slim
ENV PYTHON=/usr/bin/python3
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip g++ build-essential libsqlite3-dev && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --break-system-packages --no-cache-dir mkdocs-techdocs-core

USER node
WORKDIR /app
ENV NODE_ENV=production
ENV NODE_OPTIONS="--no-node-snapshot"

# Yarn itself is vendored (.yarn/releases/yarn-4.13.0.cjs, pinned via
# .yarnrc.yml's yarnPath) — without these, `yarn` below falls back to the
# image's global Yarn 1.x, which can't read the workspace lockfile.
COPY --chown=node:node --from=build /app/.yarn ./.yarn
COPY --chown=node:node --from=build /app/.yarnrc.yml /app/backstage.json ./

# Repo skeleton (every workspace's package.json + yarn.lock) first, so
# `yarn install` resolves the full dependency tree before the app code
# layer invalidates the cache.
COPY --chown=node:node --from=build /app/yarn.lock /app/package.json /app/packages/backend/dist/skeleton.tar.gz ./
RUN tar xzf skeleton.tar.gz && rm skeleton.tar.gz

RUN --mount=type=cache,target=/home/node/.cache/yarn,sharing=locked,uid=1000,gid=1000 \
    yarn workspaces focus --all --production && rm -rf "$(yarn cache clean)"

COPY --chown=node:node --from=build /app/packages/backend/dist/bundle.tar.gz ./
RUN tar xzf bundle.tar.gz && rm bundle.tar.gz

# Copied as explicit filenames from the local build context, not a glob
# combined with --from=build — that combination silently matched zero
# files when tried here (only the other, literal source copied, no
# error), leaving the image with no app-config.yaml at all.
COPY --chown=node:node backstage/app-config.yaml backstage/app-config.production.yaml ./

# The built frontend, served by @backstage/plugin-app-backend from the same
# process/port as the API — no separate nginx/static container needed.
COPY --chown=node:node --from=build /app/packages/app/dist ./packages/app/dist

# Catalog source, at the image root — see the matching COPY in the build
# stage's comment for why root, not /app.
COPY --chown=node:node --from=build /catalog-info.yaml /mkdocs.yml /
COPY --chown=node:node --from=build /docs/ /docs/

EXPOSE 7007
CMD ["node", "packages/backend", "--config", "app-config.yaml"]
