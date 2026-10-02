FROM node:16-alpine
WORKDIR /app
COPY . . 
#COPY ../utils ../utils
RUN npm install
COPY ./token.template.js ./token.js
CMD ["node", "index"]