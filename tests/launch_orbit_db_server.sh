cd $(mktemp -d)

git clone https://github.com/orbitdb/orbit-db-http-api.git
cd orbit-db-http-api
npm install
node src/cli.js local --orbitdb-dir ./orbitdb-data --no-https
