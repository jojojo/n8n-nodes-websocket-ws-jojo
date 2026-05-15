import {
	INodeType,
	INodeTypeDescription,
	ITriggerFunctions,
	ITriggerResponse,
	NodeOperationError,
} from 'n8n-workflow';

import WebSocket from 'ws';

export class WebsocketTrigger implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Websocket Reconnect - Trigger',
		name: 'websocketReconnectTrigger',
		icon: 'file:websocket.svg',
		group: ['trigger'],
		version: 1,
		description: 'Connect to ws endpoint and trigger flow on incoming message, open or close. Supports header/query auth and auto-reconnect.',
		defaults: {
			name: 'Websocket Connection & Message',
		},
		inputs: [],
		outputs: ['main'],
		properties: [
			{
				displayName: 'Websocket URL',
				name: 'websocketUrl',
				type: 'string',
				default: '',
				placeholder: 'wss://example.com/ws',
				description: 'URL of the WebSocket server',
			},
			{
				displayName: 'Authentication',
				name: 'authentication',
				type: 'options',
				options: [
					{ name: 'None', value: 'none' },
					{ name: 'Header (Bearer / API Key)', value: 'headerAuth' },
					{ name: 'Query Parameter', value: 'queryAuth' },
				],
				default: 'none',
				description: 'Authentication method',
			},
			{
				displayName: 'Header Name',
				name: 'headerName',
				type: 'string',
				default: 'Authorization',
				displayOptions: {
					show: { authentication: ['headerAuth'] },
				},
				description: 'Name of the HTTP header (e.g. Authorization)',
			},
			{
				displayName: 'Header Value',
				name: 'headerValue',
				type: 'string',
				typeOptions: { password: true },
				default: '',
				displayOptions: {
					show: { authentication: ['headerAuth'] },
				},
				description: 'Value of the header (e.g. Bearer mytoken)',
			},
			{
				displayName: 'Query Param Name',
				name: 'queryParamName',
				type: 'string',
				default: 'token',
				displayOptions: {
					show: { authentication: ['queryAuth'] },
				},
				description: 'Name of the query parameter',
			},
			{
				displayName: 'Query Param Value',
				name: 'queryParamValue',
				type: 'string',
				typeOptions: { password: true },
				default: '',
				displayOptions: {
					show: { authentication: ['queryAuth'] },
				},
				description: 'Value of the query parameter',
			},
			{
				displayName: 'Auto Reconnect',
				name: 'autoReconnect',
				type: 'boolean',
				default: true,
				description: 'Whether to automatically reconnect if the connection drops',
			},
			{
				displayName: 'Reconnect Interval (Seconds)',
				name: 'reconnectInterval',
				type: 'number',
				default: 10,
				displayOptions: {
					show: { autoReconnect: [true] },
				},
				description: 'Seconds to wait before attempting reconnection',
			},
			{
				displayName: 'Max Reconnect Attempts',
				name: 'maxReconnectAttempts',
				type: 'number',
				default: 0,
				displayOptions: {
					show: { autoReconnect: [true] },
				},
				description: 'Maximum reconnection attempts. 0 = unlimited.',
			},
			{
				displayName: 'Send Initial Message',
				name: 'sendInitMessage',
				type: 'boolean',
				default: false,
				description: 'Whether to send a message immediately upon connecting',
			},
			{
				displayName: 'Initial Message',
				name: 'initMessage',
				type: 'string',
				displayOptions: {
					show: { sendInitMessage: [true] },
				},
				required: true,
				default: '{}',
				description: 'Message to send upon connecting',
			},
			{
				displayName: 'Return WS Resource',
				name: 'returnWs',
				type: 'boolean',
				default: false,
				description: 'Whether to return the ws resource (needed to send messages back)',
			},
		],
	};

	async trigger(this: ITriggerFunctions): Promise<ITriggerResponse> {
		let ws: WebSocket | null = null;
		let reconnectAttempts = 0;
		let isClosed = false;

		const websocketUrl      = this.getNodeParameter('websocketUrl') as string;
		const authentication    = this.getNodeParameter('authentication') as string;
		const sendInitMessage   = this.getNodeParameter('sendInitMessage') as boolean;
		const returnWs          = this.getNodeParameter('returnWs') as boolean;
		const autoReconnect     = this.getNodeParameter('autoReconnect') as boolean;
		const reconnectInterval = (this.getNodeParameter('reconnectInterval') as number) * 1000;
		const maxReconnects     = this.getNodeParameter('maxReconnectAttempts') as number;

		const buildConnectionOptions = (): { url: string; headers: Record<string, string> } => {
			let url = websocketUrl;
			const headers: Record<string, string> = {};

			if (authentication === 'headerAuth') {
				const headerName  = this.getNodeParameter('headerName') as string || 'Authorization';
				const headerValue = this.getNodeParameter('headerValue') as string || '';
				headers[headerName] = headerValue;
			}

			if (authentication === 'queryAuth') {
				const paramName  = this.getNodeParameter('queryParamName') as string || 'token';
				const paramValue = this.getNodeParameter('queryParamValue') as string || '';
				const separator = url.includes('?') ? '&' : '?';
				url = `${url}${separator}${encodeURIComponent(paramName)}=${encodeURIComponent(paramValue)}`;
			}

			return { url, headers };
		};

		const startConsumer = async (): Promise<void> => {
			try {
				const { url, headers } = buildConnectionOptions();

				ws = new WebSocket(url, { headers });

				ws.on('error', (error: Error) => {
					console.warn('[websocket-ws] connection error:', error.message);
					this.emit([
						this.helpers.returnJsonArray([{ event: 'error', message: error.message }]),
					]);
				});

				ws.on('close', () => {
					console.debug('[websocket-ws] connection closed');
					this.emit([
						this.helpers.returnJsonArray([{ event: 'close' }]),
					]);

					if (autoReconnect && !isClosed) {
						const canRetry = maxReconnects === 0 || reconnectAttempts < maxReconnects;
						if (canRetry) {
							reconnectAttempts++;
							console.debug(`[websocket-ws] reconnecting in ${reconnectInterval / 1000}s (attempt ${reconnectAttempts})`);
							setTimeout(() => {
								startConsumer();
							}, reconnectInterval);
						} else {
							console.warn(`[websocket-ws] max reconnect attempts (${maxReconnects}) reached`);
						}
					}
				});

				ws.on('message', (data: Buffer | string, isBinary: boolean) => {
					console.debug('[websocket-ws] received new message');
					let message: any = isBinary ? data : data.toString();
					try {
						message = JSON.parse(message as string);
					} catch {
						console.warn('[websocket-ws] message is not JSON:', message);
					}

					reconnectAttempts = 0;

					this.emit([
						this.helpers.returnJsonArray([
							{
								event: 'message',
								message,
								ws: returnWs ? ws : null,
							},
						]),
					]);
				});

				ws.on('open', () => {
					console.debug('[websocket-ws] connected');
					reconnectAttempts = 0;

					if (sendInitMessage) {
						const initMessage = this.getNodeParameter('initMessage') as string;
						ws!.send(initMessage);
					}

					this.emit([
						this.helpers.returnJsonArray([
							{
								event: 'open',
								ws: returnWs ? ws : null,
							},
						]),
					]);
				});

			} catch (error: any) {
				throw new NodeOperationError(
					this.getNode(),
					`Execution error: ${error.message}`,
				);
			}
		};

		await startConsumer();

		async function closeFunction() {
			isClosed = true;
			if (ws) ws.terminate();
		}

		async function manualTriggerFunction() {
			await startConsumer();
		}

		return {
			closeFunction,
			manualTriggerFunction,
		};
	}
}
