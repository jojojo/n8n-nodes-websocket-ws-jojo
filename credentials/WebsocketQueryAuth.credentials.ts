import { ICredentialType, INodeProperties } from 'n8n-workflow';

export class WebsocketQueryAuth implements ICredentialType {
	name = 'websocketQueryAuthApi';
	displayName = 'WebSocket Query Auth';
	properties: INodeProperties[] = [
		{
			displayName: 'Parameter Name',
			name: 'name',
			type: 'string',
			default: 'token',
			description: 'Name of the query parameter',
		},
		{
			displayName: 'Parameter Value',
			name: 'value',
			type: 'string',
			typeOptions: { password: true },
			default: '',
			description: 'Value of the query parameter',
		},
	];
}
