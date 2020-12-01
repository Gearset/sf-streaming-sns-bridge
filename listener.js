const jsforce = require('jsforce');

const STATUS_INITIALIZED = "Initialized";
const STATUS_STARTING = "Starting";
const STATUS_STARTED = "Started";
const STATUS_STOPPING = "Stopping";
const STATUS_STOPPED = "Stopped";

class Listener {
    /**
     * 
     * @param {*} log
     * @param {ReplayStore} replayStore
     * @param {Publisher} publisher
     * @param {string} workerId
     * @param {*} sfConnOptions 
     * @param {string} channelName
     * @param {*} options  {replayIdStoreTableName, replayIdStoreKeyName, replayIdStoreDelay, initialReplayId}
     */
    constructor(log, replayStore, publisher, workerId, sfConnOptions, channelName, options) {
        this.log = log;

        this.replayIdManager = replayStore;

        this.publisher = publisher;

        this.workerId = workerId;

        this.sfConnOptions = sfConnOptions;

        this.channelName = channelName;
        
        this.debug = options && options.debug;

        this.status = STATUS_INITIALIZED;
        this.replayId = null;
        
        this.logMessages = new Array(20);
        this.recentMessages = new Array(20);

        this.connection = null;
        this.streamingClient = null;
        this.subscription = null;
    }

    buildStatusDTO() {
        return {
            status: this.status,
            replayId: this.replayId,
            log: this.logMessages,
            recentMessages: this.recentMessages,
        }
    }

    log(message) {
        this.logMessages.unshift({time: new Date().toISOString(), message});
        this.logMessages.pop();
    }

    logReceivedMessage(replayId) {
        this.recentMessages.unshift({time: new Date().toISOString(), replayId});
        this.recentMessages.pop();
    }

    subscribeCallback(data) {
        if (this.status !== STATUS_STOPPING && this.status != STATUS_STOPPED) {
            const newReplayId = data.event.replayId;
            const logMeta = {worker: this.workerId, replayId: newReplayId};
            if (this.debug) {
                this.log.debug(`[${this.workerId}] Received message`, {...logMeta, data});
            }
            const previousReplayId = this.replayId;
            this.logReceivedMessage(newReplayId);
            const payload = data.payload;
            const payloadJson = JSON.stringify(payload);
            this.publisher.publishToSNS(payloadJson).then(() => {
                if(this.debug) {
                    this.log.debug(`[${this.workerId}] Published to SNS (replayId=${newReplayId})`, {...logMeta, payload});
                }
                if (this.replayId === previousReplayId || this.replayId < newReplayId) {
                    this.replayId = newReplayId;
                    this.replaydIdManager.storeReplayId();
                } else {
                    this.log.debug(`replayId not updated because: previous=${previousReplayId}, new=${newReplayId}, current=${this.replayId}`, {...logMeta});
                }
            })
            .catch(e => {
                this.log(`Failed to publish to SNS (replayId=${newReplayId}): ${e}`);
                if (this.debug) {
                    this.log.debug(`[${this.workerId}] Failed to publish to SNS (replayId=${newReplayId})`, {...logMeta, error: e});
                }
            });
        }
    }

    restart() {
        this.stop();
        this.start();
    }

    start() {
        this.log.info(`Starting: ${this.workerId}`);
        this.status = STATUS_STARTING;
        this.log('Fetching initial replayId');
        return this.replayIdManager.fetchReplayId().then(replayId => {
            this.replayId = replayId;
            const replayExt = new jsforce.StreamingExtension.Replay(this.channelName, this.replayId);
            const authFailureExt = new jsforce.StreamingExtension.AuthFailure(() => {
                this.log('Restart needed because of auth error (probably expired)')
                setTimeout(this.restart.bind(this), 0);
            });
            
            this.connection = new jsforce.Connection(this.sfConnOptions);
            this.log('Logging in Salesforce')
            return this.connection.login(this.sfConnOptions.username, this.sfConnOptions.password + this.sfConnOptions.token)
                    .then(userInfo => {
                        this.log(`Creating streaming client for '${this.channelName}' with initial replayId ${this.replayId}`);
                        this.streamingClient = this.connection.streaming.createClient([ authFailureExt, replayExt ]);
                        this.log(`Creating subscription`);
                        this.subscription = this.streamingClient.subscribe(this.channelName, this.subscribeCallback.bind(this));
                        this.status = STATUS_STARTED;
                        this.log('Subscription created');
                    });
        });
    }

    stop() {
        this.log.info(`Stopping: ${this.workerId}`);
        this.status = STATUS_STOPPING;
        for (let i = 0; i < 5 && this.subscription != null; i ++) {
            this.log('Cancelling subscription');
            this.log.info(`Cancelling subscription: ${this.workerId}`);
            try {
                this.subscription.cancel();
                this.log('Cancelled subscription');
                this.subscription = null;
            } catch (err) {
                this.log('Failed to cancel subscription');
                this.log.warn(`Failed to cancel subscription: ${this.workerId}`);
            }
        }
        if (this.subscription != null) {
            this.log('Failed to cancel subscription after retries');
            this.log.error(`Failed to cancel subscription after retries: ${this.workerId}`);
            this.subscription = null;
        }
        this.status = STATUS_STOPPED;
        this.log('Stopped');
        this.log.info(`Stopped: ${this.workerId}`);
        this.streamingClient = null;
        this.connection = null;
        return this.replayIdManager.storeReplayId(true);
    }
}

module.exports = { Listener };