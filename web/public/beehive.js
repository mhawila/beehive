var MERGE_WEB_SOCKET = null;
function getBasePath() {
    return window.location.protocol + "//" + window.location.host + "/"
}

function createConfigurationObject() {
    var configuration = {
        "source": {
            "host": $('#source-host').val(),
            "username": $('#source-username').val(),
            "password": $('#source-password').val(),
            "openmrsDb": $('#source-openmrs-db').val(),
            "location": $('#source-location').val()
        },
        "destination": {
            "host": $('#destination-host').val(),
            "username": $('#destination-username').val(),
            "password": $('#destination-password').val(),
            "openmrsDb": $('#destination-openmrs-db').val()
        },
        "batchSize": $('#batch-size').val(),
        "generateNewUuids": true,
        "debug": true,
        "persist": true
    };

    if($('#generate-new-uuid').prop('checked')) {
        configuration["generateNewUuids"] = true;
    } else {
        configuration["generateNewUuids"] = false;
    }

    if($('#debug').prop('checked')) {
        configuration["debug"] = true;
    } else {
        configuration["debug"] = false;
    }

    if($('#persist').prop('checked')) {
        configuration["persist"] = true
    } else {
        configuration["persist"] = false
    }

    return configuration;
}

function toggleButtonDisabling() {
    var configButton = $('#configuration-button')
    if(configButton.attr('disabled')) {
        configButton.removeAttr('disabled') 
    } else {
        configButton.attr('disabled', true)
    }

    var dryRunMergeButton = $('#dry-run-button')
    if(dryRunMergeButton.attr('disabled')) {
        dryRunMergeButton.removeAttr('disabled') 
    } else {
        dryRunMergeButton.attr('disabled', true)
    }

    var runMergeButton = $('#run-button')
    if(runMergeButton.attr('disabled')) {
        runMergeButton.removeAttr('disabled') 
    } else {
        runMergeButton.attr('disabled', true)
    }
}


function makeWebsocketConnection(dryRun) {
    if(dryRun === undefined || dryRun === null) {
        dryRun = true;
    }
    
    var sock = $('#socket')
    // clear logs if any.
    sock.html('')

    var logMergeLogMessage = function (messageObject) {
        var color = 'color:';
        if(messageObject.category) {
            switch(messageObject.category) {
                case 'ERROR': color += 'red;'; break;
                case 'OK': color += 'green;'; break;
                case 'DEBUG': color += 'yellow;'; break;
                default: color += 'white;';
            }
        }
        var li = $('<li style="' + color + '">').html(messageObject.message);
        $('#socket').append(li);
    }

    var logString = function (message) {
        var li = $('<li style="color:white;">').html(new Date().toISOString()+ ' ' + message)
        $('#socket').append(li);
    }

    logString('Opening socket connection...')
    $('#merge-logs-panel').css('display','')
    if(MERGE_WEB_SOCKET === null) {
        toggleButtonDisabling();
        MERGE_WEB_SOCKET = new WebSocket('ws://' + window.location.host + '/running?dryRun=' + dryRun)
        console.log('See the socket', MERGE_WEB_SOCKET)
        MERGE_WEB_SOCKET.addEventListener('open', logString.bind(null, "Connection opened"))
        MERGE_WEB_SOCKET.addEventListener('error', logString.bind(null, "some error happened"))
        MERGE_WEB_SOCKET.addEventListener('message', function(message) {
            if(message.data === 'end') {
                MERGE_WEB_SOCKET.close();
                MERGE_WEB_SOCKET = null;
                toggleButtonDisabling();
            }

            try {
                var parsedData = JSON.parse(message.data)
                if(Array.isArray(parsedData)) {
                    parsedData.forEach(function(dataElement) {
                        logMergeLogMessage(dataElement)
                    })
                } else {
                    logMergeLogMessage(parsedData)
                }
            } catch (e) {
                logString(message.data)
            }
        })
    } 
}

$(document).ready(function() {
    $('#collapseOne').collapse("hide");

    $.getJSON('configuration', function(data) {
        if(typeof data === 'object') {
            $('#source-host').val(data.source.host)
            $('#source-username').val(data.source.username)
            $('#source-password').val(data.source.password)
            $('#source-openmrs-db').val(data.source.openmrsDb)

            $('#destination-host').val(data.destination.host)
            $('#destination-username').val(data.destination.username)
            $('#destination-password').val(data.destination.password)
            $('#destination-openmrs-db').val(data.destination.openmrsDb)

            $('#source-location').val(data.source.location)
            $('#batch-size').val(data.batchSize)

            if(data.generateNewUuids) {
                $('#generate-new-uuid').prop('checked', true)
            } else {
                $('#generate-new-uuid').prop('checked', false)
            }

            if(data.debug) {
                $('#debug').prop('checked', true)
            } else {
                $('#debug').prop('checked', false)
            }

            if(data.persist) {
                $('#persist').prop('checked', true)
            } else {
                $('#persist').prop('checked', false)
            }
        }
    })

    $.getJSON('sources', function(data) {
        if(Array.isArray(data)) {
            var sourcesRowsBody = $('#sources-rows')
            var sn = 1
            data.forEach(function(source) {
                var sourceRow = $('<tr>')
                    sourceRow.append('<th>' + sn)
                    sourceRow.append('<td>' + source['source'])
                if(source['atomic_step'] === 'post-obs' && source['passed'] == 1) {
                    sourceRow.append('<td><span class="label label-success">Completed</span></td>')
                } else {
                    sourceRow.append('<td><span class="label label-warning">Incomplete</span></td>')
                }
                sourcesRowsBody.append(sourceRow)
                sn++
            })
        }
    })

    // Check if there is already a merge going on on the server
    $.getJSON('check-running', function(response) {
        if(response.running) {
            // Connect to receive updates.
            makeWebsocketConnection()
        }
    })
})

$('#configuration-button').click(function() {
    var button = $(this);
    if(button.val() === 'Edit Configuration') {
        $('.configuration-form').removeAttr('disabled')
        button.removeClass('outline')
        button.val('Save Changes')
    } else {
        // Saving new stuff.
        createConfigurationObject()
        $.post({
            url: 'configuration',
            data: JSON.stringify(createConfigurationObject()),
            dataType: 'json',
            contentType: 'application/json',
            success: function(data) {
                $('.configuration-form').attr('disabled', true)
                button.addClass('outline')
                button.val('Edit Configuration')
                $('#configuration-save-success').css('display', '')
            },
            error: function(error) {
                $('#configuration-save-error').css('display', '')
            }
        })
    }
})

$('#dry-run-button').click(function() {
    makeWebsocketConnection(true)
})

$('#confirm-merge-button').click(function() {
    $('#merge-confirm-modal').modal('hide')
    makeWebsocketConnection(false)
})