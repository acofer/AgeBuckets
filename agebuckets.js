/* jslint esversion:6 */
(function(){
	"use strict";

	const cluster = require("cluster");
	const fs = require("fs");
	const readline = require("readline");
	const options = require("commander");

	/* define a default file size in bytes below which file will be read by one processor,
	 * and above which file will be divided among multiple processors and results reduced.
	 * This can be overridden with the option "-p <n>"
	 * NB: this is a rule of thumb based on tests with 8-cores and SSD, not Amdahl analysis.
	 */
	const parallel_threshhold = (10 * 1048576); // 10 MiB

	var buckets = {};
	function bucketOut(){
		for(let i in buckets){
			console.log(i + "," + buckets[i]);
		}
	}


	function serial(filename){
		// if the file is small, use the simplest approach: reading one line at a time
		var rs = fs.createReadStream(filename, {encoding: "utf8"});
		rs.once("error", (err) => { throw (err); });
		var rl = readline.createInterface({
			input: rs
		});
		var sp, id, age;
		rl.on("line", (line)=>{
			processLine(line, buckets);
		});
		rl.on("close", ()=>{ bucketOut(); });
		return;
	}


	function masterNode(filename, whenDone){
		/* start child processes and farm work out to them, whenDone is callback */
		const numCPUs = require('os').cpus().length;
		var ocpu = options.cpus;
		var cpus = (ocpu && ocpu >= 1) ? ocpu : numCPUs;
		if (cpus > numCPUs){ cpus = numCPUs; }

		var worker_buckets = [];
		var workers_reporting = 0;
		function workerMessage(msg){
			// get message back from worker with tally
			worker_buckets.push(msg);
			workers_reporting++;
			if (workers_reporting >= cpus){
				for (let b of worker_buckets){
					// b is the result from a single process { worker, buckets }; reduce
					for (let j in b.buckets){
						if (!buckets[j]){ buckets[j] = 0; }
						buckets[j] += b.buckets[j];
					}
				}
				// console.log(JSON.stringify(worker_buckets));
				whenDone();
			}
		}

		// start worker processes
		var workers = [], worker;
		for (let i = 0; i < cpus; i++){ 
			worker = cluster.fork();
			worker.on("message", workerMessage);
			workers.push(worker);
		}

		// divide up the input file into ranges, message each worker with its range
		var each = Math.floor(stat.size / cpus);
		var start = 0, end = each - 1, endi;
		for (let i in workers){
			// at the end, we'll be off by -cpu_opt bytes, so assign remainder to last worker
			endi = (+i === workers.length - 1) ? stat.size - 1 : end;
			workers[i].send({filename: filename, start: start, end: endi});
			start += each;
			end += each;
		}
	}


	function processLine(line, buckets){
		// given an array of characters, process it as a line of input
		var sp = line.split(","), age;
		if (sp.length >= 2){
			age = sp[1].trim();
			if (!buckets[age]){ buckets[age] = 0; }
			buckets[age]++;
		}
	}


	function workerNode(){
		var id = cluster.worker.id, buckets = {}, stream, line;
		process.on("message", (msg) => {
			// console.log(id + " worker from " + msg.start + " to " + msg.end);
			var position = msg.start;
			var filename = msg.filename;
			fs.open(filename, 'r', (err, fd) =>{
				if (err){ console.log("Worker could not open file: " + err); process.exit(0); }
				// In practice, here is where I would start using the async module to avoid callback hell
				stream = fs.createReadStream(null, { encoding: "utf8", fd: fd, start: position });
				const rl = readline.createInterface({
					input: stream,
				});

				var first_line = true, buf;
				rl.on('line', (line) => {
					if (first_line && position > 0){
						/* then this is not the first line of the file; discard line and
						 * count on the previous process to read the end of its last line */
						// Get character before position, if it is "\n", proceed normally, else skip line
						var p = Buffer.alloc(1);
						fs.readSync(fd, p, 0, 1, position - 1);
						// console.log(id + " first line: " + line);
						if (p.toString() !== "\n"){
							buf = new Buffer(line);
							position += buf.length + 1;
							// console.log(id + " discarding first line.");
							first_line = false;
							return;
						}
					}
					first_line = false;

					if (position <= msg.end){
						// process this line and continue
						processLine(line, buckets);
					}
					buf = new Buffer(line);
					position += buf.length + 1;

					if (position > msg.end){
						// send tallies back to master node and exit
						rl.close();
						process.send({ worker: id, buckets: buckets });
						process.exit(0);
					}
				});
				stream.on('end', ()=>{
					// process having last part of file will hit 'end' event before msg.end
					// stream.on('end') will autoClose the file
					process.send({ worker: id, buckets: buckets });
					process.exit(0);
				});
			});
		});
	}


	try{
		options
			.version("0.0.1")
			.usage("[options] <file>")
			.option("-c, --cpus <n>", "Maximum number of CPU cores to use.", parseInt)
			.option("-p, --parallel_at <n>", "Size of file in bytes above which work will be divided in parallel", parseInt)
			.parse(process.argv);
		var filename;
		if (!options.args || options.args.length < 1){
			console.log("Usage: node agebuckets.js [filename]");
			process.exit(0);
		} else {
			filename = options.args[options.args.length - 1];
		}

		const parallel_at = options.parallel_at || parallel_threshhold;
		var stat = fs.statSync(filename);

		/* decide whether to divide up the work based on file size */
		if (!stat || stat.size <= 0){
			console.log("Empty file.");
			return;
		}

		/* small file, regular approach: read file by line and tally counts */
		if (stat.size < parallel_at){
			serial(filename);
		}

		/* get fancy with our multi-core processor */
		if (stat.size >= parallel_at){
			if (cluster.isMaster){
				console.log("Parallel");
				masterNode(filename, bucketOut);
			} else {
				workerNode();
			}
		}

	} catch(ex){
		console.log("Exception in execution: " + ex);
	}
	return;
})();