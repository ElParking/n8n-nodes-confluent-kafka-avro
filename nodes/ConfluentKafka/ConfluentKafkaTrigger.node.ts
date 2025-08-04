import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import type { KafkaConfig, SASLOptions } from 'kafkajs';
import { Kafka as apacheKafka, logLevel } from 'kafkajs';
import type {
	ITriggerFunctions,
	IDataObject,
	INodeType,
	INodeTypeDescription,
	ITriggerResponse,
	IRun,
} from 'n8n-workflow';
import { NodeConnectionType, NodeOperationError } from 'n8n-workflow';

export class ConfluentKafkaTrigger implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Confluent Kafka Trigger',
		name: 'confluentKafkaTrigger',
		icon: 'file:confluent.svg',
		group: ['trigger'],
		version: 1,
		description: 'Consume messages from a Kafka topic using authenticated confluent schema registry to deserialize avro',
		defaults: {
			name: 'Confluent Kafka Trigger',
		},
		inputs: [],
		outputs: [NodeConnectionType.Main],
		credentials: [
			{
				name: 'confluentKafka',
				required: true
			},
			{
				name: `confluentSchemaRegistryApi`,
				displayName: "Confluent schema registry credentials to deserialize avro",
				displayOptions: {
					hide: {
						useSchemaRegistry: [false]
					}
				}
			},
		],
		properties: [
			{
				displayName: 'Topic',
				name: 'topic',
				type: 'string',
				default: '',
				required: true,
				placeholder: 'topic-name',
				description: 'Name of the queue of topic to consume from',
			},
			{
				displayName: 'Group ID',
				name: 'groupId',
				type: 'string',
				default: '',
				required: true,
				placeholder: 'n8n-kafka',
				description: 'ID of the consumer group',
			},
			{
				displayName: 'Use Schema Registry',
				name: 'useSchemaRegistry',
				type: 'boolean',
				default: false,
				description: 'Whether to use Confluent Schema Registry',
			},
			{
				displayName: 'Options',
				name: 'options',
				type: 'collection',
				default: {},
				placeholder: 'Add option',
				// eslint-disable-next-line n8n-nodes-base/node-param-collection-type-unsorted-items
				options: [
					{
						displayName: 'Allow Topic Creation',
						name: 'allowAutoTopicCreation',
						type: 'boolean',
						default: false,
						description: 'Whether to allow sending message to a previously non exisiting topic',
					},
					{
						displayName: 'Auto Commit Threshold',
						name: 'autoCommitThreshold',
						type: 'number',
						default: 0,
						description:
							'The consumer will commit offsets after resolving a given number of messages',
					},
					{
						displayName: 'Auto Commit Interval',
						name: 'autoCommitInterval',
						type: 'number',
						default: 0,
						description:
							'The consumer will commit offsets after a given period, for example, five seconds',
						hint: 'Value in milliseconds',
					},
					{
						displayName: 'Heartbeat Interval',
						name: 'heartbeatInterval',
						type: 'number',
						default: 3000,
						description: "Heartbeats are used to ensure that the consumer's session stays active",
						hint: 'The value must be set lower than Session Timeout',
					},
					{
						displayName: 'Max Number of Requests',
						name: 'maxInFlightRequests',
						type: 'number',
						default: 1,
						description:
							'The maximum number of unacknowledged requests the client will send on a single connection',
					},
					{
						displayName: 'Read Messages From Beginning',
						name: 'fromBeginning',
						type: 'boolean',
						default: true,
						description: 'Whether to read message from beginning',
					},
					{
						displayName: 'JSON Parse Message',
						name: 'jsonParseMessage',
						type: 'boolean',
						default: false,
						description: 'Whether to try to parse the message to an object',
					},
					{
						displayName: 'Parallel Processing',
						name: 'parallelProcessing',
						type: 'boolean',
						default: true,
						description:
							'Whether to process messages in parallel or by keeping the message in order',
					},
					{
						displayName: 'Only Message',
						name: 'onlyMessage',
						type: 'boolean',
						displayOptions: {
							show: {
								jsonParseMessage: [true],
							},
						},
						default: false,
						description: 'Whether to return only the message property',
					},
					{
						displayName: 'Return Headers',
						name: 'returnHeaders',
						type: 'boolean',
						default: false,
						description: 'Whether to return the headers received from Kafka',
					},
					{
						displayName: 'Session Timeout',
						name: 'sessionTimeout',
						type: 'number',
						default: 30000,
						description: 'The time to await a response in ms',
						hint: 'Value in milliseconds',
					},
					{
						displayName: 'Max Parallel Messages',
						name: 'maxParallelMessages',
						type: 'number',
						default: 1,
						description: 'Maximum number of messages to process in parallel when parallelProcessing is disabled',
						displayOptions: {
							show: {
								parallelProcessing: [false],
							},
						},
					},
				],
			},
		],
	};

	async trigger(this: ITriggerFunctions): Promise<ITriggerResponse> {
		const topic = this.getNodeParameter('topic') as string;

		const groupId = this.getNodeParameter('groupId') as string;

		const credentials = await this.getCredentials('confluentKafka');

		const brokers = ((credentials.brokers as string) ?? '').split(',').map((item) => item.trim());

		const clientId = credentials.clientId as string;

		const ssl = credentials.ssl as boolean;

		const options = this.getNodeParameter('options', {}) as IDataObject;

		const maxParallelMessages = options.maxParallelMessages as number || 1;

		const promiseArray: Promise<IRun>[] = [];

		var activeMessages = 0;

		const config: KafkaConfig = {
			clientId,
			brokers,
			ssl,
			logLevel: logLevel.ERROR,
		};

		if (credentials.authentication === true) {
			if (!(credentials.username && credentials.password)) {
				throw new NodeOperationError(
					this.getNode(),
					'Username and password are required for authentication',
				);
			}
			config.sasl = {
				username: credentials.username as string,
				password: credentials.password as string,
				mechanism: credentials.saslMechanism as string,
			} as SASLOptions;
		}

		const maxInFlightRequests = (
			this.getNodeParameter('options.maxInFlightRequests', null) === 0
				? null
				: this.getNodeParameter('options.maxInFlightRequests', null)
		) as number;

		const parallelProcessing = options.parallelProcessing as boolean;

		const useSchemaRegistry = this.getNodeParameter('useSchemaRegistry', 0) as boolean;

		const kafka = new apacheKafka(config);
		const consumer = kafka.consumer({
			groupId,
			maxInFlightRequests,
			sessionTimeout: this.getNodeParameter('options.sessionTimeout', 30000) as number,
			heartbeatInterval: this.getNodeParameter('options.heartbeatInterval', 3000) as number,
		});

		// The "closeFunction" function gets called by n8n whenever
		// the workflow gets deactivated and can so clean up.
		async function closeFunction() {
			try {
				await consumer.disconnect();
				console.log('Consumer disconnected successfully');
			} catch (error) {
				console.error('Error disconnecting consumer:', error);
			}
		}

		const startConsumer = async () => {
			await consumer.connect();

			await consumer.subscribe({ topic, fromBeginning: options.fromBeginning ? true : false });
			await consumer.run({
				autoCommitInterval: (options.autoCommitInterval as number) || null,
				autoCommitThreshold: (options.autoCommitThreshold as number) || null,
				eachMessage: async ({ topic: messageTopic, message }) => {
					let data: IDataObject = {};
					let value = message.value?.toString() as string;
					activeMessages++;

					if (useSchemaRegistry) {
						try {
							const sr_credentials = await this.getCredentials('confluentSchemaRegistryApi') as {
								username: string;
								password: string;
								endpoint: string;
							};
							const registry = new SchemaRegistry({ host: sr_credentials.endpoint, auth: {
								username: sr_credentials.username,
								password: sr_credentials.password,
							},  });
							value = await registry.decode(message.value as Buffer);
						} catch (error) {}
					}

					if (options.jsonParseMessage) {
						try {
							value = JSON.parse(value);
						} catch (error) {}
					}

					if (options.returnHeaders && message.headers) {
						data.headers = Object.fromEntries(
							Object.entries(message.headers).map(([headerKey, headerValue]) => [
								headerKey,
								headerValue?.toString('utf8') ?? '',
							]),
						);
					}

					data.message = value;
					data.topic = messageTopic;

					if (options.onlyMessage) {
						//@ts-ignore
						data = value;
					}
					let responsePromise = undefined;
					if (!parallelProcessing) {
						responsePromise = this.helpers.createDeferredPromise<IRun>();
						this.emit([this.helpers.returnJsonArray([data])], undefined, responsePromise);
						promiseArray.push(responsePromise.promise);
					} else {
						this.emit([this.helpers.returnJsonArray([data])]);
					}
					if (activeMessages >= maxParallelMessages) {
						for(const promise of promiseArray) {
							await promise;
							activeMessages--;
						}
						promiseArray.length = 0; // Clear the array after processing
					}
				},
			});
		};

		if (this.getMode() !== 'manual') {
			await startConsumer();
			return { closeFunction };
		} else {
			// The "manualTriggerFunction" function gets called by n8n
			// when a user is in the workflow editor and starts the
			// workflow manually. So the function has to make sure that
			// the emit() gets called with similar data like when it
			// would trigger by itself so that the user knows what data
			// to expect.
			async function manualTriggerFunction() {
				await startConsumer();
			}

			return {
				closeFunction,
				manualTriggerFunction,
			};
		}
	}
}
