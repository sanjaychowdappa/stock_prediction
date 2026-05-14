FROM node:18-alpine

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev

COPY server.js ./
COPY client_build ./client_build/

EXPOSE 3000

CMD ["node", "server.js"]
