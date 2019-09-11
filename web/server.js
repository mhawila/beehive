'use strict';
const fs = require('fs')
const express = require('express')
const bodyParser = require('body-parser')

const port = 3000
const CONFIG_DIR = '../';

const app = express()

// parse application/x-www-form-urlencoded
app.use(bodyParser.urlencoded({ extended: false }))

// parse application/json
app.use(bodyParser.json())

app.route('/configuration')
    .get((req, res) => {
        let config = require('../config')
        res.json(config)
    })
    .post((req, res) => {
        // Archive the existing configuration if any.
        // let existingConfig = require('../config')
        // if(existingConfig) {
        //     let configArchiveDir = CONFIG_DIR + 'config-archive';
        //     if(!fs.existsSync(configArchiveDir)) {
        //         fs.mkdirSync(configArchiveDir, '0755')
        //     }
        //     console.log('nothing happening here...')
        // }
        console.log('Configuration:', req.body)
        res.json({status: 'success'})
    })

app.get('/', (req, res) => res.sendFile(__dirname + '/index.html'))
app.listen(port, () => console.log(`Beehive listening on port ${port}...`))
