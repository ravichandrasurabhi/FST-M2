# Specify Base Image
FROM docker.io/node:18-alpine

# Work Directory
WORKDIR /usr/app

#copy package.json
copy package.json ./

# Install Dependencies
RUN npm install

# COPY Files
COPY ./ ./

# Startup Command
CMD ["npm", "start"]
