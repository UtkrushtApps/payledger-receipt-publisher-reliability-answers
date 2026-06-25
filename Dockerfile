FROM node:18-alpine

WORKDIR /root/task

RUN apk add --no-cache curl

COPY package.json ./
RUN npm install --omit=dev

COPY . .

EXPOSE 3000 3100

CMD ["node", "src/index.js", "api"]
