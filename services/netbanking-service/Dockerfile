FROM node:18-alpine

WORKDIR /app

COPY tsconfig.base.json ./tsconfig.base.json
COPY common/lib ./common/lib
COPY services/netbanking-service ./services/netbanking-service

# Install and build shared lib
WORKDIR /app/common/lib
RUN npm install && npm run build

# Install and build service
WORKDIR /app/services/netbanking-service
RUN npm install && npm run build

# Debug: List the dist directory
RUN ls -l /app/services/netbanking-service/dist || echo "dist folder missing"

CMD ["node", "dist/index.js"] 