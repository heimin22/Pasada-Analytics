FROM node:18-slim

WORKDIR /app

# Install dependencies (including dev dependencies for build)
COPY package*.json ./
RUN npm ci --ignore-scripts

# Copy source code
COPY . .

# Build TypeScript
RUN npm run build

# Remove dev dependencies after build
RUN npm prune --production

# Create non-root user
RUN groupadd --gid 1001 --system nodejs \
    && useradd --uid 1001 --system --gid nodejs --shell /bin/bash --create-home analytics

# Clean up unnecessary files
RUN rm -rf src tsconfig.json .eslintrc.js jest.config.js node_modules/.cache

# Change ownership and switch user
RUN chown -R analytics:nodejs /app
USER analytics

EXPOSE 3001
CMD ["npm", "start"]