/**
 * Simple SOLR client example. It is a native client it does not use any external SOLR library.
 * @author Janos Vajda
 */
var http = require('http'),
https = require('https'),
JSONStream = require('JSONStream'),
request = require('request'),
querystring = require('querystring'),
JSONbigInteger = require('json-bigint');


/**
 * Select HTTP/HTTPS protocols
 * @param {type} secure
 * @returns {https|http}
 */
function selectProtocol(secure){
   return secure ? https : http;
};

/**
 * Send JSON string to SOLR server
 * @param {type} params
 * @param {type} callback
 * @returns {sendJSON.request}
 */
function sendJSON(params,callback){
   var headers = {
      'content-type' : 'application/json; charset=utf-8',
      'content-length':  Buffer.byteLength(params.json),
      'accept' : 'application/json; charset=utf-8'
   };
   
   if(params.authorization){
      headers['authorization'] = params.authorization;
   }
   
   var options = {
      host : params.host,
      port : params.port,
      method : 'POST',
      headers : headers,
      path : params.fullPath
   };
   if(params.agent !== undefined){
      options.agent = params.agent;
   }
   var request = selectProtocol(params.secure).request(options);
   var bigint = false;
   request.on('response', handleJSONResponse(request, bigint, callback));

   request.on('error',function onError(err){
      if (callback) callback(err,null);
   });

   request.write(params.json);
   request.end();

   return request;
};

function handleJSONResponse(request, bigint, callback){
   return function onJSONResponse(response){
      var text = '';
      var err = null;
      var data = null;

      response.setEncoding('utf-8');
      response.on('data',function(chunk){
         text += chunk;
      });
      response.on('end',function(){
         if(response.statusCode < 200 || response.statusCode > 299){
            err = new SolrError(request,response,text);
            if(callback)  callback(err,null);
         }else{
            try{
               data = JSONbigInteger.parse(text);
            }catch(error){
               err = error;
            }finally{
               if(callback)  callback(err,data);
            }
         }
      });
   };
};


function SolrClient(host, port, core, path, agent, secure, bigint){

this.options={ 
  'host': 'localhost',
  'port': '8001',
  'core': '',
  'path': '/solr',
  'agent': undefined,
  'secure': false,
  'bigint': false };

}

SolrClient.prototype.add = function(docs,options,callback){
   if(typeof(options) === 'function'){
      callback = options;
      options = {};
   }
   docs = Array.isArray(docs) ? docs : [docs];
   return this.update(docs,options,callback);
}


SolrClient.prototype.update = function(data,options,callback){
   if(typeof(options) === 'function'){
      callback = options;
      options = {};
   }


var json = JSONbigInteger.stringify(data);

options=
{ 'host': 'localhost',
  'port': '8001',
  'fullPath': '/solr/update/json?&wt=json',
  'json': json,
  'secure': false,
  'bigint': false,
  'authorization': undefined,
  'agent': undefined };

var bigint = false;



var json = JSONbigInteger.stringify(data);
var SOLR_PATH = [this.options.path,this.options.core, 'update/json?' + querystring.stringify(options) +'&wt=json&commit=true']
                              .filter(function(element){
                                 return element;
                              })
                              .join('/');


   var params = {
      host : this.options.host,
      port : this.options.port,
      fullPath : SOLR_PATH,
      json : json,
      secure : this.options.secure,
      bigint : this.options.bigint,
      authorization : this.options.authorization,
      agent : this.options.agent
   };

   return sendJSON(params,callback);
}


SolrClient.prototype.commit = function(options,callback){
   if(typeof(options) === 'function'){
      callback = options;
      options = {};
   }
   var data = {
      commit : options || {}
   };
   return this.update(data,callback);
}


SolrClient.prototype.search = function(query,callback){

var options2=
{ 'host': 'localhost',
  'port': '8001',
  'path': '/solr/select?q=id:195415&start=0&rows=10&wt=json',
  'headers': { 'accept': 'application/json; charset=utf-8' }
};

   var request = selectProtocol(false).get(options2);

   request.on('response', handleJSONResponse(request, false, callback));

   request.on('error',function onError(err){
      if (callback) callback(err,null);
   });
   request.write('json');
   request.end();

}



/**
 * Demo. How to use.
 * @type SolrClient
 */
var client = new SolrClient('localhost','8001');
var options={};

client.add({ id : 123456789, title_t : 'Hello' },options, function(err,obj){
   if(err){
      console.log(err);
   }else{
      console.log('Solr response:', obj);
   }
});
