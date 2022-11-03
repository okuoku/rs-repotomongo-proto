const { MongoClient } = require("mongodb");
const uri = "mongodb://127.0.0.1:27017";
const client = new MongoClient(uri);
const fs = require("fs");
const path = require("path");
const child_process = require("child_process");
const threads = 16;
const db_name = "my_mapping";
const { Worker, parentPort, isMainThread, workerData } = require("node:worker_threads");
const xml_convert = require("xml-js");
const EMPTY = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";
const MAX_BUFFER = 1024*1024*1024*10;

//const commit = "0f8ba2ab0e7867ee121fbbd9bd1da950fe24df3e";
const gitref = "master";

let total_doc = 0;

async function resolveref(gitdir, refname){
    // Run git show-ref -s <ref>
    // Strip result
    const p = new Promise((res, rej) => {
        child_process.execFile("git", ["show-ref", "-s", refname],
                               {cwd: gitdir, maxBuffer: MAX_BUFFER},
                               (err, stdout, stderr) => {
                                   if(! err){
                                       res(stdout.trim());
                                   }else{
                                       rej(err);
                                   }
                               });
    });
    const ret = await p;
    return ret;
}

async function difftree(gitdir, from, to){
    // Run git diff-tree --name-only -r from..to
    const p = new Promise((res, rej) => {
        child_process.execFile("git", ["diff-tree", "--name-only", "-r",
            from + ".." + to], {cwd: gitdir, maxBuffer: MAX_BUFFER}, (err, stdout, stderr) => {
                if(! err){
                    res(stdout.trim().split("\n"));
                }else{
                    rej(err);
                }
            });

    });
    const ret = await p;
    return ret;
}

async function catfile(gitdir, rev, path){
    // Return false when file was not found
    const r = rev + ":" + path;
    const p = new Promise((res, rej) => {
        child_process.execFile("git", ["cat-file", "-p", rev + ":" + path],
                               {cwd: gitdir, maxBuffer: MAX_BUFFER}, (err, stdout, stderr) => {
                                   if(! err){
                                       res(stdout);
                                   }else{
                                       res(false);
                                   }
                               });
    });
    const ret = await p;
    return ret;
}

async function runrepo(cfg){

    let commit = false;
    commit = await resolveref(cfg.gitdir, cfg.branches[0]);
    console.log("ref", commit);
    let lis = await difftree(cfg.gitdir, EMPTY, commit);

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
            console.log("in feed", filtered.length, queue.length);
            if(filtered.length !== 0){
                const pth = filtered.pop();
                //console.log("Feed", pth);
                queue.push(pth);
                w.postMessage({path: pth});
            }else{
                w.postMessage({term: true});
            }
        }

        function done(pth){
            queue = queue.filter(e => e != pth);
            //console.log("done", queue);
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
                //console.log("msg",i,msg);
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
        let text = false;
        try {
            text = await catfile(cfg.gitdir, cfg.commit, pth);
        } catch(e) {
            console.dir(e);
            return false;
        }
        switch(ext){
            case ".xml":
                return xml_convert.xml2js(text, {compact: true});
            case ".json":
                const out = JSON.parse(text);
                return out;
            default:
                return text.trim().split("\n");
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
                }else{
                    console.log("deleted", obj.path);
                }
            } catch(e) {
                await client.close();
                console.dir(e);
                process.exit(1);
            }
            parentPort.postMessage({done: obj.path});
            parentPort.postMessage({feedme: true});
        }else if(obj.term){
            //console.log("term worker");
            await client.close();
            process.exit(0);
        }else if(obj.run){
            //console.log("run worker");
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
