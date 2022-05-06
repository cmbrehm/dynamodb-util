const aws=require('aws-sdk'),
  util = require('util'),
  nReadlines = require('n-readlines');

const ddbclient= new aws.DynamoDB();
const delay = util.promisify(setTimeout);

const DynamoTypes = {
  "_default": "S",
  "unixReviewTime": "N",
  "helpful": "L",
  "helpful0": "N",
  "helpful1": "N"
}

const TABLE_NAME="amz_music_reviews"
const TABLE_NOT_CREATED="UNKNOWN_TABLE";
const INSERT_BATCH_SIZE=20;
const tableParams = {
  TableName: TABLE_NAME
}

getTableStatus= async()=>{
  let d;
  try {
    d = await ddbclient.describeTable(tableParams).promise();
  } finally {
    if (! (d && d.Table)) return TABLE_NOT_CREATED;
    else return d.Table.TableStatus;
  }
}

const createParams = {
    AttributeDefinitions: [
    {
      AttributeName: "reviewerID",
      AttributeType: "S"
    },
    {
      AttributeName: "asin",
      AttributeType: "S"
    }
    ],
    KeySchema: [
     {
       AttributeName: "reviewerID",
       KeyType: "HASH"
     },
     {
       AttributeName: "asin",
       KeyType: "RANGE"
     }
   ],
  BillingMode: "PAY_PER_REQUEST",
  TableName: TABLE_NAME
};

buildTable = async() => {
  let tableStatus = await getTableStatus();
  console.log("tableStatus", tableStatus);
  if (tableStatus != "ACTIVE") {
    if (tableStatus==TABLE_NOT_CREATED) {
      await ddbclient.createTable(createParams).promise();
    }
    return delay(3000).then(buildTable());
  } else {
    return tableStatus;
  }

}

dynDataType = (k) => {
  return !!DynamoTypes[k] ? DynamoTypes[k] : DynamoTypes['_default'];
}

dynamoXform = (a) => {
  if (a instanceof Object) {
    Object.entries(a).forEach(([k,v])=>{
      let nk = dynDataType(k);
      a[k]={};
      if (nk==="L") {
        a[k][nk] = v.map((x,idx) => {
          let sk = dynDataType(`${k}${idx}`);
          return JSON.parse(`{ "${sk}" : "${x}" }`);
        })
      }
      else a[k][nk] = stringify(v); //stringify
    })
    return a;
  }
}

stringify= (a) => {
  //this is good for L but not NS or SS.
  if (Array.isArray(a))
    return a.map(x=>dynamoXform(x))
  else return a+"";
}

batchWriteDynamo = async (twenty) => {
  let m = twenty.map((i)=>{
    return {
      PutRequest: {
        Item: i
      }
    }
  });
  let params = JSON.parse(`{"RequestItems": { "${TABLE_NAME}": ${JSON.stringify(m)} } }`);
  try {
    //params.RequestItems[TABLE_NAME].forEach(r=>console.log(r.PutRequest.Item.asin.S));
    //console.log(JSON.stringify(params,6));
    let rtv = await ddbclient.batchWriteItem(params).promise();
    if (rtv.UnprocessedItems) {
      console.log("unprocessed items",rtv.UnprocessedItems);
    }
  } catch (x) {
    console.error('batch write error',x)
    throw x;
  }
}

loadTable = async() => {
  let processed = errors = 0;
  let insertBuff=[];
  const reader = new nReadlines("Musical_Instruments_5.json")
  let line;
  while (line = reader.next()) {
    try {
      let i = JSON.parse(line);
      insertBuff[processed % INSERT_BATCH_SIZE]=dynamoXform(i)
      ++processed;
    } catch (x) {
      console.log(" line error ",x)
      ++errors;
    }

    if (insertBuff.length == INSERT_BATCH_SIZE) {
      //process
      await batchWriteDynamo(Array.from(insertBuff));
      console.log(`insert ${insertBuff.length} of ${processed}`)
      insertBuff=[];
    }

  }
  console.log(`loadTable done processed ${processed} errors ${errors}`);

}

(async() => {
  await buildTable();
  await loadTable();
})()
