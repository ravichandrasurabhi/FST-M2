# Specify Base Image
FROM docker.io/node:18-alpine

# Work Directory
WORKDIR /usr/app
#copy package.json
COPY package.json ./
# Install Dependencies
RUN npm install
#copy the other files oin the container
COPY ./ ./

# Startup Command
CMD ["npm", "start"]