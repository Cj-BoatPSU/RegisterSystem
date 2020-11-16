const zmq = require("zeromq")
const redis = require('redis')
const client = redis.createClient()
const sock_pub = new zmq.Publisher
const sock_sub = new zmq.Subscriber
sock_pub.bind("tcp://127.0.0.1:3001")
var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://127.0.0.1:40000,127.0.0.1:40001,127.0.0.1:40002/?replicaSet=regapp&readPreference=secondary"

async function subscribe() {
  sock_sub.connect("tcp://127.0.0.1:3000")
  sock_sub.subscribe("Register")
  console.log("Subscriber connected to port 3000")
  for await (const [topic, msg] of sock_sub) {
    client.incr("worker", function(err, reply){
        if(reply == 1){
            console.log(`Topic: ${topic} Subject ID: ${msg}`)
            client.set("worker", 0)
            publish('Registering')
            if(topic == "Register"){
                client.get(msg + "Current", function(err, reged){
                  client.get(msg + "Max", function(err, max_reg){
                    if(reged < max_reg){
                      console.log(reged)
                      client.incr(msg + "Current", function(err, reply){
                        if(reply <= max_reg){
                          publish("Success : " + reply) 
                          MongoClient.connect(url, { useNewUrlParser: true, useUnifiedTopology: true },
                            (err, db) => {
                              if (err) throw err;
                              var dbo = db.db("AllInformation");
                              dbo.collection("Subject information").update({ "SubjectID": msg.toString() }, { $inc: { "NumberofStudentRegistered": 1 } }, (err, subject) => {
                                if (err) throw err;
                                db.close();
                              });
                            }
                          );
                        }
                      })
                    }
                    else{
                      publish("FULL")
                    }
                  })
                })       
            }
            else if(topic == "Withdraw"){
              client.decr(msg + "Current", function(err, reply){
                      publish("Success : " + reply) 
                      MongoClient.connect(url, { useNewUrlParser: true, useUnifiedTopology: true },
                        (err, db) => {
                          if (err) throw err;
                          var dbo = db.db("AllInformation");
                          dbo.collection("Subject information").update({ "SubjectID": msg.toString() }, { $inc: { "NumberofStudentRegistered": -1 } }, (err, subject) => {
                            if (err) throw err;
                            db.close();
                          });
                        }
                      );
              })
            }
          }
        })
      }
      
}
async function publish(msg) {
  console.log("Publisher bound to port 3001 " + msg)
  await sock_pub.send([`Respond`, `${msg}`])
}

subscribe()