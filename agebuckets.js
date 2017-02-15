/* jslint esversion:6 */
(function(){
	"use strict";

	const fs = require("fs");
	const readline = require("readline");
	const options = require("commander");


	try{
		options
			.version("0.0.1")
			.option("-i, --in", "Name of input file.")
			.option("-p, --parallel_at", "Size of file in bytes above which work will be divided in parallel")
			.parse(process.argv);
		// TODO: cpu core limit option

		const filename = options.in || "test.txt";
		const parallel_at = options.parallel_at || 1048576; // 1 MiB
		var stat = fs.statSync(filename);

		/* decide whether to divide up the work based on file size */
		if (stat.size <= 0){
			console.log("Empty file.");
			return;
		}
		var buckets = {};

		/* small file, regular approach: read file by line and tally counts */
		if (stat.size < parallel_at){
			var rs = fs.createReadStream(filename);
			rs.once("error", (err) => { throw (err); });
			var rl = readline.createInterface({
				input: rs
			});
			var sp, id, age;
			rl.on("line", (line)=>{
				sp = line.split(",");
				if (sp.length < 2){ return; }
				age = sp[1].trim();
				if (!buckets[age]){ buckets[age] = 0; }
				buckets[age]++;
			});
			rl.on("close", ()=>{
				for(let i in buckets){
					console.log(i + "," + buckets[i]);
				}
			});
		}

		/* get fancy with our multi-core processor */
		if (stat.size >= parallel_at){
			const cluster = require("cluster");
			const numCPUs = require('os').cpus().length;

			if (cluster.isMaster){
				/* divide up the input file into ranges, read, tally, and send back counts as JSON */
				var mbuckets = [];
				var workers_reporting = 0;
				var worker_message = function(msg){
					// get message back from worker, should be tally
					mbuckets.push(msg);
					workers_reporting++;
					if (workers_reporting >= numCPUs){
						// console.log(JSON.stringify(mbuckets));
						for (let b of mbuckets){
							// b is the result from a single process, iterate through it
							for (let j in b.buckets){
								if (!buckets[j]){ buckets[j] = 0; }
								buckets[j] += b.buckets[j];
							}
						}
						for(let i in buckets){
							console.log(i + "," + buckets[i]);
						}
					}
				};

				var worker_exit = function(){};

				var workers = [], worker;
				for (let i = 0; i < numCPUs; i++){ 
					worker = cluster.fork();
					worker.on("message", worker_message);
					worker.on("exit", worker_exit);
					workers.push(worker);
				}

				var each = Math.floor(stat.size / numCPUs);
				var start = 0, end = each - 1, endi;
				for (let i in workers){
					// at the end, we'll be off by -numCPUs bytes, so assign remainder to last worker
					endi = (+i === workers.length - 1) ? stat.size - 1 : end;
					workers[i].send({start: start, end: endi});
					start += each;
					end += each;
				}
			} else {
				// console.log("Worker " + process.pid + " started");
				process.on("message", (msg) => {
					fs.open(filename, 'r', (err, fd) =>{
						if (err){ console.log("Worker could not open file: " + err); process.exit(0); }
						// In practice, here is where I would start using the async module to avoid callback hell
						var position = msg.start, stream, line;
						var buckets = {};
						stream = fs.createReadStream(null, { encoding: "utf8", fd: fd, start: position });
						stream.on('readable', () => {
							var chunk, linea = [], age;
							if (position > 0){
								// then we didn't get the first line of the file; discard line
								while (null !== (chunk = stream.read(1)) && chunk !== "\n") { position += chunk.length; }
							}

							function processLine(linea, buckets){
								var line = linea.join("").split(",");
								if (line.length >= 2){
									age = line[1].trim();
									if (!buckets[age]){ buckets[age] = 0; }
									buckets[age]++;
								}
								// console.log(cluster.worker.id + ": " + line);
							}

							while (null !== (chunk = stream.read(1))) {
								if (chunk === "\n"){
									// End of line, process this line and reset
									processLine(linea, buckets);
									linea = [];
								} else {
									linea.push(chunk);
								}
								position += chunk.length;
								if (position >= msg.end){
									// read the rest of the line and exit
									if (linea.length > 0){
										// there is part of a line in our buffer, read the rest of it
										var line_end = false;
										while (null !== (chunk = stream.read(1)) && !line_end){
											if (chunk !== "\n"){
												linea.push(chunk);
											} else {
												processLine(linea, buckets);
												line_end = true;
											}
										}
									}
									fs.close(fd);
									process.send({ worker: cluster.worker.id, buckets: buckets });
									process.exit(0);
								}
							}
						});
						// stream.on('end') will autoClose the file, we don't need to do it
						stream.on('end', ()=>{
							process.send({ worker: cluster.worker.id, buckets: buckets });
							process.exit(0);
						});
					});
				});
			}
		}

	} catch(ex){
		console.log("Unable to read file. " + ex);
	}
	return;
})();