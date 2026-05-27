import { ICredentialType, INodeProperties } from 'n8n-workflow';

export class WebsocketQueryAuthApi implements ICredentialType {
	name = 'websocketQueryAuthApi';
	displayName = 'WebSocket Query Auth';
	properties: INodeProperties[] = [
		{
			displayName: 'Parameter Name',
			name: 'paramName',
			type: 'string',
			default: 'token',
			description: 'Query parameter name',
		},
		{
			displayName: 'Parameter Value',
			name: 'paramValue',
			type: 'string',
			typeOptions: { password: true },
			default: '',
			description: 'Query parameter value',
		},
	];
}
