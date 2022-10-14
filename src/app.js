"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
exports.__esModule = true;
var ip = require("ip");
var kafkajs_1 = require("kafkajs");
var host = process.env.HOST_IP || ip.address();
var kafka = new kafkajs_1.Kafka({
    //logLevel: logLevel.DEBUG,
    //clientId: 'example-consumer',
    brokers: ["".concat(host, ":9093")],
    sasl: {
        mechanism: 'scram-sha-256',
        username: 'user',
        password: 'TqQ9WIpiM7'
    }
});
var topic = 'FLINK_TOPIC';
var consumer = kafka.consumer({ groupId: 'GROUP' });
var run = function () { return __awaiter(void 0, void 0, void 0, function () {
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0: return [4 /*yield*/, consumer.connect()];
            case 1:
                _a.sent();
                return [4 /*yield*/, consumer.subscribe({ topic: topic, fromBeginning: true })];
            case 2:
                _a.sent();
                return [4 /*yield*/, consumer.run({
                        // eachBatch: async ({ batch }) => {
                        //   console.log(batch)
                        // },
                        eachMessage: function (_a) {
                            var topic = _a.topic, partition = _a.partition, message = _a.message;
                            return __awaiter(void 0, void 0, void 0, function () {
                                var prefix;
                                return __generator(this, function (_b) {
                                    prefix = "".concat(topic, "[").concat(partition, " | ").concat(message.offset, "] / ").concat(message.timestamp);
                                    console.log("- ".concat(prefix, " ").concat(message.key, "#").concat(message.value));
                                    return [2 /*return*/];
                                });
                            });
                        }
                    })];
            case 3:
                _a.sent();
                return [2 /*return*/];
        }
    });
}); };
console.log("-------------------------------------");
console.log("Welcome to the simple Kafka Consumer ");
console.log("-------------------------------------");
console.log("Host IP-Address: ", host);
console.log(" ");
run()["catch"](function (e) { return console.error("[example/consumer] ".concat(e.message), e); });
var errorTypes = ['unhandledRejection', 'uncaughtException'];
var signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];
errorTypes.forEach(function (type) {
    process.on(type, function (e) { return __awaiter(void 0, void 0, void 0, function () {
        var _1;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _a.trys.push([0, 2, , 3]);
                    console.log("process.on ".concat(type));
                    console.error(e);
                    return [4 /*yield*/, consumer.disconnect()];
                case 1:
                    _a.sent();
                    process.exit(0);
                    return [3 /*break*/, 3];
                case 2:
                    _1 = _a.sent();
                    process.exit(1);
                    return [3 /*break*/, 3];
                case 3: return [2 /*return*/];
            }
        });
    }); });
});
signalTraps.forEach(function (type) {
    process.once(type, function () { return __awaiter(void 0, void 0, void 0, function () {
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _a.trys.push([0, , 2, 3]);
                    return [4 /*yield*/, consumer.disconnect()];
                case 1:
                    _a.sent();
                    return [3 /*break*/, 3];
                case 2:
                    process.kill(process.pid, type);
                    return [7 /*endfinally*/];
                case 3: return [2 /*return*/];
            }
        });
    }); });
});
