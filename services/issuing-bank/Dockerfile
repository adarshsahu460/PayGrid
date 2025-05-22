FROM node:18-alpine

WORKDIR /app

COPY tsconfig.base.json ./tsconfig.base.json
COPY common/lib ./common/lib
COPY services/issuing-bank ./services/issuing-bank

# Install and build shared lib
WORKDIR /app/common/lib
RUN npm install && npm run build

# Install and build service
WORKDIR /app/services/issuing-bank
RUN npm install && npm run build

# Debug: List the dist directory
RUN ls -l /app/services/issuing-bank/dist || echo "dist folder missing"

# Set production environment only after build (optional)
# ENV NODE_ENV=production

CMD ["node", "dist/index.js"] 