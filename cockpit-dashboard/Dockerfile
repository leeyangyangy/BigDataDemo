# syntax=docker/dockerfile:1
# ============ build stage ============
FROM node:22-alpine AS build
WORKDIR /build
ENV NODE_ENV=production

COPY package.json package-lock.json ./
RUN npm ci --include=dev

COPY . .
RUN npm run build

# ============ runtime stage ============
FROM nginx:1.27-alpine

COPY --from=build /build/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
