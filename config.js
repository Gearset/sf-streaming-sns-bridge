const AWS = require('aws-sdk');

class Config {
    get() {
        let configString = process.env.BRIDGE_CONFIG;
        let configStringPromise;
        if (configString == null) {
            const parameterName = process.env.BRIDGE_CONFIG_PARAMETER_STORE;
            if (parameterName != null) {
                configStringPromise = new AWS.SSM()
                    .getParameter({Name: parameterName, WithDecryption: true})
                    .promise()
                    .then(r => r.Parameter.Value)
                    .catch(err => {
                        throw new Error(`Failed to get parameter '${parameterName}' from AWS: ${err}`);
                    });
            } else {
                throw new Error(`No configuration can be found in environment variable either 'BRIDGE_CONFIG' or 'BRIDGE_CONFIG_PARAMETER_STORE'`);
            }
        } else {
            configStringPromise = Promise.resolve(configString);
        }
        return configStringPromise.then(config => {
            let obj;
            try {
                obj = JSON.parse(config);
            } catch (err) {
                throw new Error(`Failed to parse JSON configuration (${err}): ${config}`)
            };
            if (obj.options == null) {
                obj.options = {};
            }

            [
                ['replayIdStoreTableName', 'REPLAY_ID_STORE_TABLE_NAME'],
                ['replayIdStoreKeyName', 'REPLAY_ID_STORE_KEY_NAME'],
                ['replayIdStoreDelay', 'REPLAY_ID_STORE_DELAY'],
                ['initialReplayId', 'INITIAL_REPLAY_ID'],
                ['debug', 'DEBUG']
            ].forEach(([name, envName]) => {
                const fullEnvName = 'BRIDGE_CONFIG_' + envName
                const v = process.env[fullEnvName];
                if (v) {
                    obj.options[name] = v;
                    log.info(`Configuration 'options.${name}' overridden by value read from environment variable '${fullEnvName}': ${v}`);
                }
            });
            return obj;
        });
    }
}

module.exports = { Config };