FROM node:18-alpine

WORKDIR /app

COPY tsconfig.base.json ./tsconfig.base.json
COPY common/lib ./common/lib
COPY services/payment-service ./services/payment-service
COPY services/payment-service/schema.sql ./services/payment-service/schema.sql

# Install and build shared lib
WORKDIR /app/common/lib
RUN npm install && npm run build

# Install and build service
WORKDIR /app/services/payment-service
RUN npm install && npm run build

# Debug: List the dist directory
RUN ls -l /app/services/payment-service/dist || echo "dist folder missing"

# Set production environment only after build (optional)
# ENV NODE_ENV=production

# To initialize the database, run the following command manually or via an init script:
# psql -h <postgres_host> -U <postgres_user> -d <postgres_db> -f ./services/payment-service/schema.sql

CMD ["node", "dist/index.js"] 