import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import type { KafkaConfig, SASLOptions, TopicMessages } from 'kafkajs';
import { CompressionTypes, Kafka as apacheKafka } from 'kafkajs';
import type {
	IExecuteFunctions,
	IDataObject,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	IPairedItemData,
} from 'n8n-workflow';
import { NodeConnectionType, NodeOperationError } from 'n8n-workflow';

function generatePairedItemData(length: number): IPairedItemData[] {
	return Array.from({ length }, (_, item) => ({
		item,
	}));
}

export class ConfluentKafka implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Confluent Kafka',
		name: 'confluentKafka',
		icon: 'file:confluent.svg',
		group: ['transform'],
		version: 1,
		description: 'Sends messages to a Kafka topic',
		defaults: {
			name: 'Confluent Kafka',
		},
		usableAsTool: true,
		inputs: [NodeConnectionType.Main],
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
				placeholder: 'topic-name',
				description: 'Name of the queue of topic to publish to',
			},
			{
				displayName: 'Send Input Data',
				name: 'sendInputData',
				type: 'boolean',
				default: true,
				description: 'Whether to send the data the node receives as JSON to Kafka',
			},
			{
				displayName: 'Message',
				name: 'message',
				type: 'string',
				displayOptions: {
					show: {
						sendInputData: [false],
					},
				},
				default: '',
				description: 'The message to be sent',
			},
			{
				displayName: 'JSON Parameters',
				name: 'jsonParameters',
				type: 'boolean',
				default: false,
			},
			{
				displayName: 'Use Schema Registry',
				name: 'useSchemaRegistry',
				type: 'boolean',
				default: false,
				description: 'Whether to use Confluent Schema Registry',
			},
			{
				displayName: 'Use Key',
				name: 'useKey',
				type: 'boolean',
				default: false,
				description: 'Whether to use a message key',
			},
			{
				displayName: 'Key',
				name: 'key',
				type: 'string',
				required: true,
				displayOptions: {
					hide: {
						useKey: [false],
					},
				},
				placeholder: '',
				default: '',
				description: 'The message key',
			},
			{
				displayName: 'Event Name',
				name: 'eventName',
				type: 'string',
				required: true,
				displayOptions: {
					hide: {
						useSchemaRegistry: [false]
					},
				},
				default: '',
				description: 'Namespace and Name of Schema in Schema Registry (namespace.name)',
			},
			{
				displayName: 'Key Name',
				name: 'keyName',
				type: 'string',
				required: true,
				displayOptions: {
					show: {
						useSchemaRegistry: [true],
						useKey: [true]
					},
				},
				default: '',
				description: 'Namespace and Name of Schema for the key in Schema Registry (namespace.name)',
			},
			{
				displayName: 'Headers',
				name: 'headersUi',
				placeholder: 'Add Header',
				type: 'fixedCollection',
				displayOptions: {
					show: {
						jsonParameters: [false],
					},
				},
				typeOptions: {
					multipleValues: true,
				},
				default: {},
				options: [
					{
						name: 'headerValues',
						displayName: 'Header',
						values: [
							{
								displayName: 'Key',
								name: 'key',
								type: 'string',
								default: '',
							},
							{
								displayName: 'Value',
								name: 'value',
								type: 'string',
								default: '',
							},
						],
					},
				],
			},
			{
				displayName: 'Headers (JSON)',
				name: 'headerParametersJson',
				type: 'json',
				displayOptions: {
					show: {
						jsonParameters: [true],
					},
				},
				default: '',
				description: 'Header parameters as JSON (flat object)',
			},
			{
				displayName: 'Options',
				name: 'options',
				type: 'collection',
				default: {},
				placeholder: 'Add option',
				options: [
					{
						displayName: 'Acks',
						name: 'acks',
						type: 'boolean',
						default: false,
						description: 'Whether or not producer must wait for acknowledgement from all replicas',
					},
					{
						displayName: 'Compression',
						name: 'compression',
						type: 'boolean',
						default: false,
						description: 'Whether to send the data in a compressed format using the GZIP codec',
					},
					{
						displayName: 'Timeout',
						name: 'timeout',
						type: 'number',
						default: 30000,
						description: 'The time to await a response in ms',
					},
				],
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const itemData = generatePairedItemData(items.length);

		const length = items.length;

		const topicMessages: TopicMessages[] = [];

		let responseData: IDataObject[];

		try {
			const options = this.getNodeParameter('options', 0);
			const sendInputData = this.getNodeParameter('sendInputData', 0) as boolean;

			const useSchemaRegistry = this.getNodeParameter('useSchemaRegistry', 0) as boolean;

			const timeout = options.timeout as number;

			let compression = CompressionTypes.None;

			const acks = options.acks === true ? 1 : 0;

			if (options.compression === true) {
				compression = CompressionTypes.GZIP;
			}

			const credentials = await this.getCredentials('confluentKafka');

			const brokers = ((credentials.brokers as string) || '').split(',').map((item) => item.trim());

			const clientId = credentials.clientId as string;

			const ssl = credentials.ssl as boolean;

			const config: KafkaConfig = {
				clientId,
				brokers,
				ssl,
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

			const kafka = new apacheKafka(config);

			const producer = kafka.producer();

			await producer.connect();

			let message: string | Buffer;

			for (let i = 0; i < length; i++) {
				if (sendInputData) {
					message = JSON.stringify(items[i].json);
				} else {
					message = this.getNodeParameter('message', i) as string;
				}

				const useKey = this.getNodeParameter('useKey', i) as boolean;
				const key = useKey ? (this.getNodeParameter('key', i) as string) : null;
				var keyValue = null;

				if (useSchemaRegistry) {
					try {
						const eventName = this.getNodeParameter('eventName', 0) as string;
						const keyName = useKey ? this.getNodeParameter('keyName', 0) as string : null;
						const sr_credentials = await this.getCredentials('confluentSchemaRegistryApi') as {
								username: string;
								password: string;
								endpoint: string;
						};

						const registry = new SchemaRegistry({ host: sr_credentials.endpoint, auth: {
							username: sr_credentials.username,
							password: sr_credentials.password
						} });

						const id = await registry.getLatestSchemaId(eventName);
						this.logger.debug(`Latest schema for event: ${id}, event: ${message}`)
						if (keyName && key) {
							const keyId = await registry.getLatestSchemaId(keyName);
							this.logger.debug(`Latest schema for key: ${keyId}, key: ${key}`)
							keyValue = await registry.encode(keyId, JSON.parse(key));
						}
						message = await registry.encode(id, JSON.parse(message));
					} catch (exception) {
						this.logger.error(`Error in confluent kafka processing: ${exception}`)
						throw new NodeOperationError(
							this.getNode(),
							'Verify your Schema Registry configuration',
						);
					}
				}

				const topic = this.getNodeParameter('topic', i) as string;

				const jsonParameters = this.getNodeParameter('jsonParameters', i);

				let headers;

				if (jsonParameters) {
					headers = this.getNodeParameter('headerParametersJson', i) as string;
					try {
						headers = JSON.parse(headers);
					} catch (exception) {
						throw new NodeOperationError(this.getNode(), 'Headers must be a valid json');
					}
				} else {
					const values = (this.getNodeParameter('headersUi', i) as IDataObject)
						.headerValues as IDataObject[];
					headers = {};
					if (values !== undefined) {
						for (const value of values) {
							//@ts-ignore
							headers[value.key] = value.value;
						}
					}
				}
				topicMessages.push({
					topic,
					messages: [
						{
							value: message,
							headers,
							key: keyValue,
						},
					],
				});
			}

			responseData = await producer.sendBatch({
				topicMessages,
				timeout,
				compression,
				acks,
			});

			if (responseData.length === 0) {
				responseData.push({
					success: true,
				});
			}

			await producer.disconnect();

			const executionData = this.helpers.constructExecutionMetaData(
				this.helpers.returnJsonArray(responseData),
				{ itemData },
			);

			return [executionData];
		} catch (error) {
			if (this.continueOnFail()) {
				return [[{ json: { error: error.message }, pairedItem: itemData }]];
			} else {
				throw error;
			}
		}
	}
}
