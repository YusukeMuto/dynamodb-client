const AWS = require('aws-sdk');
 
//const DIR = "./system/"

//AWS.config.loadFromPath(DIR + 'credentials.json');
AWS.config.update({region: 'ap-northeast-1'});
 
const dynamo = new AWS.DynamoDB.DocumentClient();

exports.get = (params) => {
  return dynamo.get(params).promise().then((v)=>{
    return Promise.resolve(v.Item);
  });
};

exports.scn = (params) => {
  return dynamo.scan(params).promise().then((v)=>{
    return Promise.resolve(v.Items);
  });
};


exports.scnAll = (params) => {
  let result = [];
  let promises = () => {
    return dynamo.scan(params).promise().then( (val)=>{
      result = result.concat (val.Items);

      if ( val.LastEvaluatedKey )
      {
        console.log(JSON.stringify(val.LastEvaluatedKey));
        params.ExclusiveStartKey = val.LastEvaluatedKey
        return promises();
      }
      else
      {
        return Promise.resolve(result);
      }
    })
  };

  return promises();
};


exports.qry = (params) => {
  return dynamo.query(params).promise().then((v)=>{
    return Promise.resolve(v.Items);
  });
};

exports.btg = (params) =>{
  return dynamo.batchGet(params).promise().then((v)=>{
    return Promise.resolve(v.Responses);
  });
}

exports.put = (params) => {
  return dynamo.put(params).promise().then((v)=>{
    return Promise.resolve(v);
  });
};

exports.upd = (params) => {
  return dynamo.update(params).promise().then((v)=>{
    return Promise.resolve(v.Attributes);
  });
};

exports.btw = (params) =>{
  return dynamo.batchWrite(params).promise().then((v)=>{
    return Promise.resolve(v);
  });
}

exports.del = (params) => {
 return dynamo.delete(params).promise().then((v)=>{
    return Promise.resolve(v);
 });
}

exports.btd = (params) => {
  return exports.btw(params);
}


exports.btgs = (params_s) => {
 let length  = params_s.length;
 let tableName = Object.keys(params_s[0].RequestItems)[0];
 let counter = 0;

 let results = [];

 let promises = () => {
  return exports.btg(params_s[counter]).then( ( val ) => {
    results = results.concat(val[tableName]);
    counter++;
    if ( counter < length ) {
      return promises();
    } else {
      return Promise.resolve(results);
    }
   });
 };
 return promises();
}

exports.btws = (params_s) => {
 let length  = params_s.length;
 let tableName = Object.keys(params_s[0].RequestItems)[0];
 let counter = 0;

 let results = [];
 let promisess = () => {
  return exports.btw(params_s[counter]).then( ( val ) => {
    results.push(val);
    counter++;
    if ( counter < length ) {
      return promisess();
    } else {
      return Promise.resolve(results);
    }
   });
 };
 return promisess();
}

exports.upds = (params_s) => {
 let length  = params_s.length;
 let counter = 0;

 let results = [];

 let promises = () => {
  return exports.upd(params_s[counter]).then( ( val ) => {
    counter++;
    if ( counter < length ) {
      return promises();
    } else {
      return Promise.resolve();
    }
   });
 };
 return promises();
}

