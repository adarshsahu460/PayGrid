FROM node:20

WORKDIR /app

COPY tsconfig.base.json ./tsconfig.base.json
COPY common/lib ./common/lib
COPY services/psp-service ./services/psp-service

# Install and build shared lib
WORKDIR /app/common/lib
RUN npm install && npm run build

# Install and build service
WORKDIR /app/services/psp-service
RUN npm install && npm run build

# Debug: List the dist directory
RUN ls -l /app/services/psp-service/dist || echo "dist folder missing"

# Set production environment only after build (optional)
# ENV NODE_ENV=production

CMD ["node", "dist/index.js"] 