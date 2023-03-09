let req = {
	"fromBlock": 0,
	"transactions": [
		{
			"to": [
				"0x9cb7712c6a91506e69e8751fcb08e72e1256477d"
			],
			"sighash": [
				"0x8ca887ca"
			],
			"fieldSelection": {
				"block": {
					"number": true,
					"hash": true,
					"parentHash": true
				},
				"log": {
					"address": true,
					"topics": true,
					"data": true,
					"transaction": true
				},
				"transaction": {
					"from": true,
					"to": true,
					"gasPrice": true,
					"gas": true
				}
			}
		}
	],
	"logs": [
		{
			"address": [
				"0x0f98431c8ad98523631ae4a59f267346ea31f984"
			],
			"topics": [
				[
					"0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
				]
			],
			"fieldSelection": {
				"block": {
					"number": true,
					"hash": true,
					"parentHash": true
				},
				"log": {
					"address": true,
					"topics": true,
					"data": true,
					"transaction": true
				},
				"transaction": {
					"from": true,
					"to": true,
					"gasPrice": true,
					"gas": true
				}
			}
		},
		{
			"address": [
				"0xc36442b4a4522e871399cd717abdd847ab11fe88"
			],
			"topics": [
				[
					"0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f",
					"0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4",
					"0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01",
					"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
				],
				[
					"0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c",
					"0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde",
					"0x98636036cb66a9c19a37435efc1e90142190214e8abeb821bdba3f2990dd4c95",
					"0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
				]
			],
			"fieldSelection": {
				"block": {
					"number": true,
					"hash": true,
					"parentHash": true
				},
				"log": {
					"address": true,
					"topics": true,
					"data": true,
					"transaction": true
				},
				"transaction": {
					"from": true,
					"to": true,
					"gasPrice": true,
					"gas": true
				}
			}
		}
	]
};

const endTime = performance.now();

while(true) {
  const start = performance.now();
  
  const resp = await fetch("http://localhost:8080/query", {
    method: "POST",
    headers: {
        "Content-Type": "application/json",
      },
    body: JSON.stringify(req)
  });
  
  const end = performance.now();
  
  const respBody = await resp.json();
  
  const { archiveHeight, nextBlock } = respBody;
  
  console.log(`scanned ${nextBlock - req.fromBlock} blocks in ${end - start} ms`);
  
  console.log(`nextBlock: ${nextBlock}`);
  
  if (archiveHeight <= nextBlock) {
    break;
  }
  
  req.fromBlock = nextBlock;
}

const startTime = performance.now();

console.log(`Finished sync in ${endTime - startTime} milliseconds`);
