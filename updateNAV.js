// dependencies
const aws = require('aws-sdk');
aws.config.update({ region: 'eu-west-1' });
var DV = require('./dateValidator.js');
error = false;
errorMessage = [];


exports.handler = (event, context, callback) => {
    //parse the event from SNS
    console.log(event);
    var messageObj = event.Records[0].Sns.Message;
    var message = JSON.parse(messageObj);

    //console.log(messageObj);
    var dateSequence = new Date().getTime().toString();
    var dateTime = new Date().toUTCString();
    //execute the main process
    console.log("calling main", message);
    mainProcess(context, event, message.calculateSRRI, message.requestUUID, message.ICIN, message.NAV, dateSequence, dateTime, message.expectedSequence, message.category, message.frequency, message.user, message.description, message.calculationDate);
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


mainProcess = function (context, event, calculateSRRI, requestUUID, ICIN, NAV, dateSequence, dateTime, expectedSequence, category, frequency, user, description, dateIn) {
//calculate new, last and expected last sequence for the ICIN

    var newSequence = createSequence(dateIn, frequency, ICIN);
    if(error){
        raiseError((ICIN, NAV, newSequence, dateSequence, requestUUID, dateTime, user));
        return;
    }
    var lastSequence = getLastSequence(ICIN);
    var expectedLastSequence = getExpectedLastSequence(newSequence, dateIn);
    var sequenceFloor = parseInt(newSequence) - 500;
    console.log("sequence in "+newSequence);
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
    var dynamo = new aws.DynamoDB();
    var tableName = "NAVHistory";
    var item = {
        RequestUUID: {"S": requestUUID},
        ICIN: {"S" :ICIN},
        NAV: {"N": NAV},
        Frequency:{"S": frequency},
        UpdatedTimeStamp: {"N": dateSequence},
        UpdatedDateTime: {"S": dateTime},
        UpdateUser: {"S": user},
        Sequence: {"N": newSequence}
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
                var message = {
                    requestUUID: requestUUID,
                    ICIN: ICIN,
                    NAV: NAV,
                    sequence: newSequence,
                    frequency: frequency,
                    category: category,
                    user: user,
                    shareClassDescription: description
                }
                console.log("requesting calculation preparation", message);
                sendLambdaSNS(event, context, message, "arn:aws:sns:eu-west-1:437622887029:prepareCalculationRequest", "prepare calculation request");
                context.done();
            }
        }
    });
}

getLastSequence = function (ICIN) {
    var lastSequence = 123;
    return lastSequence;
}

getExpectedLastSequence = function (sequence, dateIn) {
    var expectedLastSequence;
    var week = parseInt(sequence.substr(4, 2));
    if (week > 1){
        week = week - 1;
        if (week < 9){
            expectedLastSequence = (sequence.substr(0,4)+"0"+week.toString());
        }else{
            expectedLastSequence = (sequence.substr(0,4)+week.toString());
        }
    } else{
        var date = new Date(Date.UTC(parseInt(dateIn.substr(6,4)),(parseInt(dateIn.substr(3,2)-1)),parseInt(dateIn.substr(0,2)),0,0,0));
        var year = parseInt(date.getFullYear());
        var expectedYear = (year - 1).toString();
        var DV = require('dateValidater.js');
        var weeksInYear = new DV.getWeeksInYearForYear(expectedYear);
        expectedLastSequence = expectedYear + weeksInYear;

        }
    
    return expectedLastSequence;
}

raiseError = function (ICIN, NAV, sequence, dateSequence, requestUUID, dateTime, user) {
    //write to the database
    var dynamo = new aws.DynamoDB();
    var tableName = "errorLog";
    var item = {
        RequestUUID: {"S": requestUUID},
        ICIN: {"S": ICIN},
        NAV: {"N": NAV},
        CreatedTimeStamp: {"N": dateSequence},
        CreatedDateTime: {"S": dateTime},
        CreateUser: {"S": user},
        Sequence: {"N": sequence},
        Stage: {"S": "Update NAV"},
        ErrorMessage: {"S": errorMessage}
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

createSequence = function(dateIn, frequency, ICIN){
    var dateInDate = DV.dateFactory(dateIn);
    
    if(DV.isValidDate(dateInDate)){
         var sequence = DV.sequenceFactory(dateInDate, frequency);
    } else{
        sequence = "";
        error = true;
        errorMessage.push("ICIN: "+ICIN + "- Invalid date entered, NAV not updated");
    }
    return sequence;
}


