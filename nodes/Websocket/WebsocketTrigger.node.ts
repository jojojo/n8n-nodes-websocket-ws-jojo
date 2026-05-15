import {
	ICredentialDataDecryptedObject,
	INodeType,
	INodeTypeDescription,
	ITriggerFunctions,
	ITriggerResponse,
	NodeApiError,
	NodeOperationError,
} from 'n8n-workflow';

import WebSocket from 'ws';

export class WebsocketTrigger implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Websocket - Trigger',
		name: 'websocketTrigger',
		icon: 'file:websocket.svg',
		group: ['trigger'],
		version: 1,
		description: 'Connect to ws endpoint and trigger flow on incoming message, open or close. Supports Bearer token credentials with auto-reconnect.',
		defaults: {
			name: 'Websocket Connection & Message',
		},
		inputs: [],
		outputs: ['main'],
		credentials: [
			{
				name: 'httpHeaderAuthApi',
				required: false,
				displayOptions: {
					show: {
						authentication: ['headerAuth'],
					},
				},
			},
			{
				name: 'httpQueryAuthApi',
				required: false,
				displayOptions: {
					show: {
						authentication: ['queryAuth'],
					},
				},
			},
		],
		properties: [
			{
				displayName: 'Authentication',
				name: 'authentication',
				type: 'options',
				options: [
					{ name: 'None', value: 'none' },
					{ name: 'Header Auth (Bearer / API Key)', value: 'headerAuth' },
					{ name: 'Query Parameter Auth', value: 'queryAuth' },
				],
				default: 'none',
				description: 'Authentication method to use when connecting to the WebSocket server',
			},
			{
				displayName: 'Websocket URL',
				name: 'websocketUrl',
				type: 'string',
				default: '',
				placeholder: 'wss://example.com/ws',
				description: 'URL of the WebSocket server. Do NOT include auth tokens here — use the Authentication field above.',
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
				description: 'Maximum number of reconnection attempts. 0 = unlimited.',
			},
			{
				displayName: 'Send Initial Message',
				name: 'sendInitMessage',
				type: 'boolean',
				default: false,
				description: 'Whether to send a message to the server immediately upon connecting',
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

		const buildConnectionOptions = async (): Promise<{ url: string; headers: Record<string, string> }> => {
			let url = websocketUrl;
			const headers: Record<string, string> = {};

			if (authentication === 'headerAuth') {
				const creds = await this.getCredentials('httpHeaderAuthApi') as ICredentialDataDecryptedObject;
				const headerName  = (creds.name  as string) || 'Authorization';
				const headerValue = (creds.value as string) || '';
				headers[headerName] = headerValue;
			}

			if (authentication === 'queryAuth') {
				const creds = await this.getCredentials('httpQueryAuthApi') as ICredentialDataDecryptedObject;
				const paramName  = (creds.name  as string) || 'token';
				const paramValue = (creds.value as string) || '';
				const separator = url.includes('?') ? '&' : '?';
				url = `${url}${separator}${encodeURIComponent(paramName)}=${encodeURIComponent(paramValue)}`;
			}

			return { url, headers };
		};

		const startConsumer = async (): Promise<void> => {
			try {
				const { url, headers } = await buildConnectionOptions();

				ws = new WebSocket(url, { headers });

				ws.on('error', (error: Error) => {
					console.warn('[websocket-ws] connection error:', error.message);
					const errorData = {
						message: 'WebSocket connection error',
						description: error.message,
					};
					this.emit([
						this.helpers.returnJsonArray([{ event: 'error', message: error.message }]),
					]);
					throw new NodeApiError(this.getNode(), errorData);
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
