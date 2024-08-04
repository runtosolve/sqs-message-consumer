console.log('Loading function');
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocument, PutCommand } from '@aws-sdk/lib-dynamodb';
import { fromUtf8 } from '@aws-sdk/util-utf8-node';
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";


const marshallOptions = {};
const unmarshallOptions = {};

const translateConfig = { marshallOptions, unmarshallOptions };

const client = new DynamoDBClient({});
const dbDocClient = DynamoDBDocument.from(client, translateConfig);

const secretsHardCode = { accessKeyId: process.env.AWS_ACCESS_KEY_ID, secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY }


const credentials = {
    region: 'us-east-2',
    credentials: secretsHardCode
};

const tableName = 'purlinline';
const tableNameResponse = 'responses';
const clientLambda = new LambdaClient(credentials);

export const handler = async (event) => {

    let initialTime = null


    for (let { messageId, body } of event.Records) {
        const msg = JSON.parse(body);

        initialTime = new Date(msg.initTime)
        console.log(`msg.initTime ${msg.initTime}, ${typeof msg.initTime}, ${initialTime}, ${typeof initialTime}`);

        await dbDocClient.send(
            new PutCommand({
                TableName: tableName,
                Item: {
                    purlinlineId: messageId,
                    creationDate: new Date().toString(),
                    initialTime: msg.initTime,
                    loading_direction: msg.loading_direction,
                    purlin_types: msg.purlin_types,
                    roof_slope: msg.roof_slope,
                    "status": "PROCESSING"
                },
            })
        );
        console.log('SQS message %s: %j', { messageId }, { body });

        const params = {
            FunctionName: 'mock-processor', // Replace with your child function's name
            InvocationType: 'RequestResponse',
            LogType: 'Tail',
            Payload: JSON.stringify(body),
        };

        try {
            console.log('Start invoking lambda fn', { params });
            const command = new InvokeCommand(params);
            console.log('Invoke response', { command });
            const response = await clientLambda.send(command);
            console.log('Response', { response }, response?.Payload);

            const responsePayload = JSON.stringify(response);
            const body = response?.Payload;

            const decodeBase64String = (base64String) => {
                // Decode the Base64 string to a Buffer
                const buffer = Buffer.from(base64String, 'base64');
                // Convert the Buffer to a UTF-8 string
                return JSON.parse(buffer.toString('utf-8'));
            };


            // // Example usage:
            const base64String = decodeBase64String(body);

            console.log(`base64String ${base64String}`)
            console.log(`applied_pressur ${base64String?.body?.applied_pressur}`)


            const endDate = new Date();
            // Example usage
            const difference = calculateDateDifference(initialTime, endDate);
            console.log(`endDate ${endDate}, initDate ${initialTime}, Difference: ${difference.days} days, ${difference.hours} hours, ${difference.minutes} minutes, ${difference.seconds} seconds`);

            await dbDocClient.send(
                new PutCommand({
                    TableName: tableNameResponse,
                    Item: {
                        id: messageId,
                        creationDate: endDate.toString(),
                        applied_pressur: base64String?.body?.applied_pressur,
                        delay: base64String?.body?.randomNumber,
                        timeDifference: `${difference.minutes} minutes, ${difference.seconds} seconds`,
                        STATUS: "COMPLETED"
                    },
                })
            );



            console.log(`Successfully processed messages.`);

            console.log(`responsePayload ${responsePayload}`)
            console.log(`body ${body}, ${body?.applied_pressur}`)
            return responsePayload;
        } catch (error) {
            console.error('Error invoking child function:', error);
            
            await dbDocClient.send(
            new PutCommand({
                TableName: tableName,
                Item: {
                    purlinlineId: messageId,
                    creationDate: new Date().toString(),
                    initialTime: msg.initTime,
                    loading_direction: msg.loading_direction,
                    purlin_types: msg.purlin_types,
                    roof_slope: msg.roof_slope,
                    "status": "ERROR",
                    "errorMessage": error.errorMessage
                },
            })
        );
        
            throw error;
            
        }
    }
};

export const calculateDateDifference = (startDateStr, endDateStr) => {
    // Parse the date strings into Date objects
    const startDate = new Date(startDateStr);
    const endDate = new Date(endDateStr);

    // Calculate the difference in milliseconds
    const diffInMs = endDate - startDate;

    // Convert milliseconds to days, hours, minutes, and seconds
    const diffInSeconds = Math.floor(diffInMs / 1000);
    const diffInMinutes = Math.floor(diffInSeconds / 60);
    const diffInHours = Math.floor(diffInMinutes / 60);
    const diffInDays = Math.floor(diffInHours / 24);

    const seconds = diffInSeconds % 60;
    const minutes = diffInMinutes % 60;
    const hours = diffInHours % 24;
    const days = diffInDays;

    return {
        days,
        hours,
        minutes,
        seconds,
    };
}

