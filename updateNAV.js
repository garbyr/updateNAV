// dependencies
const aws = require('aws-sdk');
aws.config.update({ region: 'eu-west-1' });


exports.handler = (event, context, callback) => {
    //parse the event from SNS
    console.log(event);
    var messageObj = event.Records[0].Sns.Message;
    var message = JSON.parse(messageObj);

    console.log(messageObj);
    var dateSequence = new Date().getTime();
    var dateTime = new Date().toUTCString();
    //execute the main process
    console.log("calling main");
    mainProcess(context, event, message.calculateSRRI, message.requestUUID, message.ICIN, message.NAV, dateSequence, dateTime, message.sequence, message.category, message.frequency, message.user, message.description);
}


sendLambdaSNS = function (event, context, message, topic, subject) {
    var sns = new aws.SNS();
    var params = {
        Message: JSON.stringify(message),
        Subject: subject,
        TopicArn: topic
    };
    sns.publish(params, context.done);
    return null;
}


mainProcess = function (context, event, calculateSRRI, requestUUID, ICIN, NAV, dateSequence, dateTime, sequence, category, frequency, user, description) {
    //check if NAV change is allowed - is it next in sequence or update of old?
    var lastSequence = getLatestNAV(ICIN);
    var expectedLastSequence = getExpectedSequence(sequence);
    var sequenceFloor = sequence - 500;
    console.log("sequence in "+sequence);
    console.log("last sequence "+lastSequence);
    console.log("expected last sequence "+expectedLastSequence);
    console.log("sequence floor "+sequenceFloor);
   /*
    if (expectedLastSequence > LastSequence) {
        raiseError(ICIN, NAV, sequence, dateSequence, requestUUID, dateTime, user, "Incorrect sequence for frequency " + frequency +" : the expected sequence > actual last sequence");
        context.fail();
        return;
    } else if (lastSequence < sequenceFloor) {
        raiseError(ICIN, NAV, sequence, dateSequence, requestUUID, dateTime, user, "Incorrect sequence for frequency " + frequency +" : the sequence is outside the the ESMA calculation range");
        context.fail();
        return;
    }
    */
    //write to the database
    var doc = require('dynamodb-doc');
    var dynamo = new doc.DynamoDB();
    var tableName = "NAVHistory";
    var item = {
        RequestUUID: requestUUID,
        ICIN: ICIN,
        NAV: parseFloat(NAV),
        UpdatedTimeStamp: parseInt(dateSequence),
        UpdatedDateTime: dateTime,
        UpdateUser: user,
        Sequence: parseInt(sequence)
    }

    var params = {
        TableName: tableName,
        Item: item
    }

    dynamo.putItem(params, function (err, data) {
        if (err) {
            console.log("ERROR", err);
            context.fail();
        }
        else {
            console.log("SUCCESS", data);
            console.log("process SRRI = ", calculateSRRI);
            if (calculateSRRI == "Yes") {
                console.log("sending calculation request");
                var message = {
                    requestUUID: requestUUID,
                    ICIN: ICIN,
                    NAV: parseFloat(NAV),
                    sequence: parseInt(sequence),
                    frequency: frequency,
                    category: category,
                    user: user,
                    description: description
                }
                console.log("requesting calculation preparation", message);
                sendLambdaSNS(event, context, message, "arn:aws:sns:eu-west-1:437622887029:prepareCalculationRequest", "prepare calculation request");
                context.done();
            }
        }
    });
}

getLatestNAV = function (ICIN) {
    var lastSequence = 123;
    return lastSequence;
}

getExpectedSequence = function (sequence) {
    var expectedLastSequence = 123;
    return expectedLastSequence;
}

raiseError = function (ICIN, NAV, sequence, dateSequence, requestUUID, dateTime, user, error) {
    //write to the database
    var doc = require('dynamodb-doc');
    var dynamo = new doc.DynamoDB();
    var tableName = "ICINErrorLog";
    var item = {
        RequestUUID: requestUUID,
        ICIN: ICIN,
        NAV: parseFloat(NAV),
        CreatedTimeStamp: parseInt(dateSequence),
        CreatedDateTime: dateTime,
        CreateUser: user,
        Sequence: parseInt(sequence),
        Stage: "Update NAV",
        Error: error
    }

    var params = {
        TableName: tableName,
        Item: item
    }

    dynamo.putItem(params, function (err, data) {
        if (err) {
            console.log("ERROR", err);
            context.fail();
        }
        else {
            console.log("SUCCESS", data);
            context.done();
        }
        
    });
}


