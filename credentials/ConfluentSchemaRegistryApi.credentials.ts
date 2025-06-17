import {
	Icon,
	ICredentialType,
	INodeProperties,
} from 'n8n-workflow';

export class ConfluentSchemaRegistryApi implements ICredentialType {
  name = 'confluentSchemaRegistryApi';
  displayName = 'Confluent Schema Registry API';
	documentationUrl = 'https://docs.confluent.io/platform/current/schema-registry/index.html';
	icon: Icon = 'file:confluent.svg';
  properties: INodeProperties[] = [
    {
      displayName: 'Schema Registry URL',
      name: 'endpoint',
      type: 'string',
      default: 'https://<tu-schema-registry>:8081',
    },
    {
      displayName: 'Username',
      name: 'username',
      type: 'string',
      default: '',
    },
    {
      displayName: 'Password',
      name: 'password',
      type: 'string',
      typeOptions: {
        password: true,
      },
      default: '',
    },
  ];
}
