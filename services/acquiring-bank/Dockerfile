FROM node:18-alpine

WORKDIR /app

COPY tsconfig.base.json ./tsconfig.base.json
COPY common/lib ./common/lib
COPY services/acquiring-bank ./services/acquiring-bank

# Install and build shared lib
WORKDIR /app/common/lib
RUN npm install && npm run build

# Install and build service
WORKDIR /app/services/acquiring-bank
RUN npm install && npm run build

# Debug: List the dist directory
RUN ls -l /app/services/acquiring-bank/dist || echo "dist folder missing"

# Set production environment only after build (optional)
# ENV NODE_ENV=production

CMD ["node", "dist/index.js"]