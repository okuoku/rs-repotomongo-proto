const { MongoClient } = require("mongodb");
const uri = "mongodb://127.0.0.1:27017";
const client = new MongoClient(uri);
const isogit = require("isomorphic-git");
const fs = require("fs");
const path = require("path");
const threads = 10;
const db_name = "my_mapping";
const { Worker, parentPort, isMainThread, workerData } = require("node:worker_threads");
const xml_convert = require("xml-js");

//const commit = "0f8ba2ab0e7867ee121fbbd9bd1da950fe24df3e";
const gitref = "master";

let total_doc = 0;

async function runrepo(cfg){
    let cache = {};
    let mapper = async function(pth, ab){
        // from https://isomorphic-git.org/docs/en/snippets
        const A = ab[0];
        const B = ab[1];
        const Atype = await A?.type();
        const Btype = await B?.type();
        if(Atype === "tree" || Btype === "tree"){
            /* Ignore trees */
            return;
        }
        const Aoid = await A?.oid();
        const Boid = await B?.oid();
        if(Aoid != Boid){
            return pth;
        }else{
            return null;
        }
    }

    let commit = false;
    commit = await isogit.resolveRef({fs, gitdir: cfg.gitdir,
                                     cache: cache, ref: gitref});

    let lis = await isogit.walk({fs, 
                                gitdir: cfg.gitdir,
                                cache: cache,
                                trees: [ isogit.TREE({ref: commit}) ],
                                map: mapper});

    /* Prefilter list by filenames */
    let filtered = lis.filter(e => {
        if(! e){
            return false;
        }
        const basename = path.basename(e);
        const idx = cfg.files.findIndex(n => n == basename);
        return idx >= 0;
    });
    let queue = [];

    total_doc = filtered.length;


    await new Promise((res, rej) => {
        const workers = [];

        function feed(w){
            console.log("in feed", filtered.length);
            if(filtered.length !== 0){
                const pth = filtered.pop();
                console.log("Feed", pth);
                queue.push(pth);
                w.postMessage({path: pth});
            }else{
                w.postMessage({term: true});
            }
        }

        function done(pth){
            queue = queue.filter(e => e != pth);
            console.log("done", queue);
            if(queue.length === 0 && filtered.length === 0){
                workers.forEach(w => {
                    w.postMessage({term: true});
                });
                res();
            }
        }

        if(filtered.length === 0){
            console.log("No objects.");
            res();
            return;
        }

        /* Run with workers */
        for(let i=0;i!=threads;i++){
            const mycfg = cfg;
            mycfg.commit = commit;
            const w = new Worker(__filename, { workerData: mycfg });
            w.on("message", msg => {
                if(msg.feedme){
                    feed(w);
                }else if(msg.done){
                    done(msg.done);
                }
            });
            w.on("error", msg => {
                throw msg;
            });
            w.on("exit", code => {
                if(code !== 0){
                    throw new Error("invalid exit code ${code}");
                }
            });
            w.postMessage({run: true});
        }
    });

    console.log("total", total_doc);

    return null;
}

async function runworker(){
    const cfg = workerData;
    const decoder = new TextDecoder();
    let cache = {};
    let col = {};
    try {
        const db = client.db(db_name);
        col = db.collection(cfg.collection);
    } catch(e) {
        await client.close();
        console.dir(e);
        process.exit(1);
    }

    async function readdoc(pth){
        const ext = path.extname(pth);
        let blob = false;
        try {
            blob = await isogit.readBlob({fs, gitdir: cfg.gitdir,
                                         cache: cache, oid: cfg.commit,
                                         filepath: pth});
        } catch(e) {
            console.dir(e);
            return false;
        }
        const text = decoder.decode(blob.blob);
        switch(ext){
            case ".xml":
                return xml_convert.xml2js(text, {compact: true});
            case ".json":
                return JSON.parse(text);
            default:
                return text.split("\n");
        }
    }

    parentPort.on("message", async function(obj) {
        if(obj.path){
            const basename = path.basename(obj.path);
            try {
                const doc = await readdoc(obj.path);
                await col.deleteOne({path: obj.path});
                if(doc){
                    await col.insertOne({path: obj.path, filename: basename, doc: doc});
                }
            } catch(e) {
                await client.close();
                console.dir(e);
                process.exit(1);
            }
            parentPort.postMessage({done: obj.path});
            parentPort.postMessage({feedme: true});
        }else if(obj.term){
            console.log("term worker");
            await client.close();
            process.exit(0);
        }else if(obj.run){
            console.log("run worker");
            parentPort.postMessage({feedme: true});
        }
    });
}

async function run(){
    try {
        const db = client.db(db_name);
        const col = db.collection("config");

        const config = await col.find({ _id: { $exists: true }})
        .toArray();
        console.log(config);
        await Promise.all(config.map(runrepo));
        return null;
    } finally {
        await client.close();
    }
}

if(isMainThread){
    run().catch(console.dir);
}else{
    runworker().catch(console.dir);
}
