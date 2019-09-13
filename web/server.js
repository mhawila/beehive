'use strict';
const fs = require('fs')
const express = require('express')
const bodyParser = require('body-parser')
const orchestration = require('../orchestration')
const MessageQueue = require('../MessageQueue');
const nodeUtils = require('../node_utils')
const sources = require('../api/sources')

global.progressMessageQueue = new MessageQueue();

const port = 3000
const CONFIG_DIR = require('path').dirname(__dirname) + '/';
const CONFIG_FILE_PATH = CONFIG_DIR + '/config.json';

const app = express()
const ws = require('express-ws')(app);

// parse application/x-www-form-urlencoded
app.use(bodyParser.urlencoded({ extended: false }))

// parse application/json
app.use(bodyParser.json())

app.route('/configuration')
    .get((req, res) => {
        let config = require(CONFIG_FILE_PATH)
        res.json(config)
    })
    .post((req, res) => {
        // Archive the existing configuration if any.
        let existingConfig = require(CONFIG_FILE_PATH)
        if(existingConfig) {
            let configArchiveDir = CONFIG_DIR + 'config-archives';
            if(!fs.existsSync(configArchiveDir)) {
                fs.mkdirSync(configArchiveDir, '0755')
            }
            let configCopy = 'config_' + new Date().toISOString() + '.json';
            fs.copyFileSync(CONFIG_FILE_PATH, configArchiveDir + '/' + configCopy)
        }
        fs.writeFileSync(CONFIG_FILE_PATH, JSON.stringify(req.body, null, 2))
        // Purge the config from require cache.
        nodeUtils.removeModuleFromRequireCache(CONFIG_FILE_PATH)

        // console.log('Configuration:', req.body)
        res.json({status: 'success'})
    })

app.get('/running', (req, res) => {
    console.log('Query string => ', req.query)
    if(req.query.dryRun !== undefined) {
        orchestration(req.query.dryRun)
    } else {
        orchestration()
    }
    res.end('done')
})

app.get('/sources', (req, res) => {
    sources.getSourcesLastStep(data => {
        res.json(data)
    })
})

app.ws('/running', (ws, req) => {
    console.log('Websocket connection');
    console.log('Query string => ', req.query)

    global.progressMessageQueue.on('enqueue', (data) => {
        ws.send(JSON.stringify(data))
    })

    global.progressMessageQueue.on('end', () => {
        // console.log('Is this called at all......?')
        ws.send('end');
    })

    // Received subsquent messages.
    ws.on('message', (data) => {
        if(data === 'dryRun') {
            _clearMessageQueueThenRun(true)
        }
    })

    if(req.query.dryRun !== undefined) {
        _clearMessageQueueThenRun(req.query.dryRun)
    } else {
        orchestration()
    }
})

app.get('/', (req, res) => res.sendFile(__dirname + '/index.html'))
app.listen(port, console.log.bind(null, `Beehive listening on port ${port}...`))

const _clearMessageQueueThenRun = (isDryRun) => {
    global.progressMessageQueue.clear()
    orchestration(isDryRun)
}
