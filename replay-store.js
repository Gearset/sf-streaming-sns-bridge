const AWS = require('aws-sdk');

class ReplayStore {

    /**
     * @param {*} options  {replayIdStoreTableName, replayIdStoreKeyName, replayIdStoreDelay, initialReplayId}
     */
    constructor(options) {
        this.replayIdStoreTableName = options && options.replayIdStoreTableName;
        this.replayIdStoreKeyName = (options && options.replayIdStoreKeyName) || 'channel';
        this.replayIdStoreDelay = (options && options.replayIdStoreDelay) || 2000;
        this.initialReplayId = (options && options.initialReplayId) || -1;
        this.lastReplayIdStoredTime = 0;
        this.lastReplayIdStored = null;

        this.ddb = new AWS.DynamoDB.DocumentClient();
    }

    /**
     * Store replay id to DynamoDB if configured, or no-op otherwise.
     * @param {boolean} flush if true the actual Promise would be returned, otherwise Promise.resolve() would be returned.
     * @returns Promise for the save operation or no-op
     */
    storeReplayId(flush) {
        if (this.replayIdStoreTableName) {
            const now = new Date().getTime();
            const moreToWait = this.replayIdStoreDelay - (now - this.lastReplayIdStoredTime);
            if (flush || moreToWait <= 0) {
                // save to DynamoDb
                const newReplayId = this.replayId;
                const p = this.ddb.update({
                    TableName: this.replayIdStoreTableName,
                    Key: {
                        [this.replayIdStoreKeyName]: this.workerId,
                    },
                    UpdateExpression: "set replayId = :newReplayId",
                    ConditionExpression: `attribute_not_exists(${this.replayIdStoreKeyName}) or attribute_not_exists(replayId) or replayId < :newReplayId`,
                    ExpressionAttributeValues:{
                        ":newReplayId": newReplayId,
                    },
                }).promise()
                .then(result => {
                    this.lastReplayIdStored = newReplayId;
                })
                .catch(e => {
                    this.log(`Didn't store replayId ${newReplayId}: ${e}`);
                });
                return flush? p : Promise.resolve();
            } else {
                setTimeout(this.storeReplayId.bind(this), moreToWait + 100);
            }
        }
        return Promise.resolve();
    }

    fetchReplayId() {
        if (this.replayIdStoreTableName) {
            return this.ddb.get({
                TableName: this.replayIdStoreTableName,
                Key: {
                    [this.replayIdStoreKeyName]: this.workerId,
                },
            }).promise()
            .then(result => {
                if (result.Item && result.Item.replayId) {
                    return result.Item.replayId;
                } else {
                    this.log(`There is no previously stored replayId, will use ${this.initialReplayId}`);
                    return this.initialReplayId;
                }
            })
            .catch(e => {
                this.log(`Couldn't fetch previously stored replayId, will use ${this.initialReplayId}: ${e}`);
                return this.initialReplayId;
            });
        }
        return Promise.resolve(this.initialReplayId);
    }
}

module.exports = { ReplayStore };