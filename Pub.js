const express = require('express');
const app = express();
const port = 8000;
const cors = require('cors');
var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://127.0.0.1:40000,127.0.0.1:40001,127.0.0.1:40002/?replicaSet=regapp&readPreference=secondary"


const zmq = require("zeromq")
const redis = require('redis')
const client = redis.createClient()
const sock_pub = new zmq.Publisher
const sock_sub = new zmq.Subscriber
client.set("worker", 0)
sock_pub.bind("tcp://127.0.0.1:3000")

app.use(cors())

app.get('/', (req, res) => {
  res.send("Hello!, Welcome to Register System");
});

app.get('/redis', (req, res) => {
  res.send("Initailization redis");
  MongoClient.connect(url, { useNewUrlParser: true, useUnifiedTopology: true },
      (err, db) => {
          if (err) throw err;
          var dbo = db.db("AllInformation");
          dbo.collection("Subject information").find({}).toArray((err, subjects) => {
              if (err) throw err;
              // res.send(subjects);
              subjects.forEach((subject, index) => {
                  client.set(subject.SubjectID + "Current", subject.NumberofStudentRegistered.toString())
                  client.set(subject.SubjectID + 'Max', subject.MaximumNumberofStudentRegistered.toString())
              });
              db.close();
          })
      }
  );
});

app.get('/subjects', (req, res) => {
  MongoClient.connect(url, { useNewUrlParser: true, useUnifiedTopology: true },
    (err, db) => {
        if (err) throw err;
        var dbo = db.db("AllInformation");
        dbo.collection("Subject information").find({}).toArray((err, subjects) => {
          if (err) throw err;
            res.send(subjects);
            db.close();
        });
    });
});

app.get('/subjects/:id', (req, res) => {
  var subject_id = req.params.id;
    MongoClient.connect(url, { useNewUrlParser: true, useUnifiedTopology: true },
        (err, db) => {
            if (err) throw err;
            var dbo = db.db("AllInformation");
            dbo.collection("Subject information").findOne({ "SubjectID": subject_id }, (err, subject) => {
                if (err) throw err;
                res.json(subject);
                db.close();
        });
    });
});

async function publish(topic, subj_id) {
  console.log(`Publisher bound to port 3000 Message: ${topic} ${subj_id}`)
  await sock_pub.send([topic, subj_id])
}

async function subscribe() {
  sock_sub.connect("tcp://127.0.0.1:3001")
  sock_sub.subscribe("Respond")
  console.log("Subscriber connected to port 3001")

  for await (const [topic, msg] of sock_sub) {
    console.log(`Topic: ${topic} Subject ID: ${msg}`)
  }
}

app.get('/subjects/:id/register', (req, res) => {
  publish("Register", req.params.id)
  res.send("Register Success!")
});

app.get('/subjects/:id/withdraw', (req, res) => {
  publish("Withdraw", req.params.id)
  res.send("Withdraw Success!")
});

subscribe()
app.listen(port, () => console.log(`Registration_System_Server is listening on port ${port}`));