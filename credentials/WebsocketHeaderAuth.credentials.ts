import { ICredentialType, INodeProperties } from 'n8n-workflow';

export class WebsocketHeaderAuth implements ICredentialType {
	name = 'websocketHeaderAuthApi';
	displayName = 'WebSocket Header Auth';
	properties: INodeProperties[] = [
		{
			displayName: 'Header Name',
			name: 'name',
			type: 'string',
			default: 'Authorization',
			description: 'Name of the HTTP header (e.g. Authorization)',
		},
		{
			displayName: 'Header Value',
			name: 'value',
			type: 'string',
			typeOptions: { password: true },
			default: '',
			description: 'Value of the header (e.g. Bearer mytoken)',
		},
	];
}
