# cc-connect-docker-mongodb

This repo builds a Docker image extended from `cc-docker-connect` that contains the MongoDB Atlas connector.

## Building locally

To build:

```bash
make build
```

To deploy an image:

```bash
make build push-docker-latest
```