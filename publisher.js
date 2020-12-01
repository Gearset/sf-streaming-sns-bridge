const AWS = require('aws-sdk');

class Publisher {
    /**
     * 
     * @param {string} snsTopicArn 
     */
    constructor(snsTopicArn) {
        this.sns = new AWS.SNS();
        this.snsTopicArn = snsTopicArn;
    }

    publishToSNS(payload) {
        return this.sns.publish({
            Message: payload,
            TopicArn: this.snsTopicArn,
        }).promise()
        .then(result => result.MessageId);
    }
}

module.exports = { Publisher };