import { ICredentialType, INodeProperties } from 'n8n-workflow';

export class WebsocketHeaderAuthApi implements ICredentialType {
	name = 'websocketHeaderAuthApi';
	displayName = 'WebSocket Header Auth';
	properties: INodeProperties[] = [
		{
			displayName: 'Header Name',
			name: 'headerName',
			type: 'string',
			default: 'Authorization',
			description: 'HTTP header name (e.g. Authorization)',
		},
		{
			displayName: 'Header Value',
			name: 'headerValue',
			type: 'string',
			typeOptions: { password: true },
			default: '',
			description: 'Header value (e.g. Bearer mytoken)',
		},
	];
}
