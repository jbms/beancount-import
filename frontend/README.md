This directory contains the source code for the Beancount-import web frontend.

The web frontend communicates with a Python server (typically running on the
same machine) over a websocket.  The Python server also serves a copy of the
frontend code.

# Development

Building the frontend requires some additional setup:

1. A recent version of [Node.js](https://nodejs.org) is required to build the
   frontend.  The recommended way to obtain Node.js is by installing NVM (node
   version manager) by following the instructions here:
   
   https://github.com/creationix/nvm

2. Install a recent version of Node.js by running:

   `nvm install stable`
    
    You can also specify a version manually.
    
3. From within this directory, install the dependencies required by this
   project:

   `npm install`

4. To build a development version of the frontend, run:

   `npm run builddev`
   
   Alternatively, you can run:
   
   `npm run builddev:watch`
   
   This watches the source tree and incrementally rebuilds as changes are made.
   This is the recommended command to use during development.
   
5. The build output is written to the
   (beancount_import/frontend_dist/)[../beancount_import/frontend_dist/]
   directory.  When using the `builddev` or `builddev:watch` commands, the
   frontend is built in development mode.  When using the `build` command, the
   frontend is built in production (minified) mode.
