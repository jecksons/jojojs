const fs = require('fs');


const processParams = {
    output: '',
    db_host: '',
    db_name: '',
    db_user: '',
    db_password: '',
    db_port: 0,
    tables: ''
}

const paramsDescriptions = {
    output: 'Output for files',
    db_host: 'Database host (default localhost)',
    db_name: 'Database name',
    db_user: 'Database user',
    db_password: 'Database password',
    db_port: 'Database port (default 3306)',
    tables: 'Tables, separated by comma'
}

const inputKeys = Object.keys(processParams);
let currInputKey = inputKeys[0];
let inputComplete = false;

console.log('*'.repeat(50));
console.log('                 Welcome to Jojo.js     ');
console.log('*'.repeat(50));
console.log('');

const processImport = () => {
    require('./import-db').process(processParams);
}

const onGetInListener = (data) => {
    let valInput = data.toString().replace('\n', '');
    if (valInput !== '' ||  currInputKey === 'db_host' || currInputKey === 'db_port' ) {
        if (currInputKey === 'output') {
            if (!fs.existsSync(valInput)) {
                process.stdout.write(paramsDescriptions[currInputKey] + ': ');             
                return;
            }
        }
        if (currInputKey === 'db_port') {
            processParams[currInputKey] = parseInt(valInput) > 0 ? parseInt(valInput) : 3306;
        } else {
            processParams[currInputKey] = valInput;
        }
        inputComplete = inputKeys.indexOf(currInputKey) === (inputKeys.length -1);
        if (!inputComplete) {
            currInputKey = inputKeys[inputKeys.indexOf(currInputKey) +1];
        } else {
            process.stdin.removeListener('data', onGetInListener);
            processImport();
        }
    }
    if (!inputComplete) {
        process.stdout.write(paramsDescriptions[currInputKey] + ': ');             
    }
}

process.stdin.on('data', onGetInListener);
process.stdout.write(paramsDescriptions[currInputKey] + ': ');