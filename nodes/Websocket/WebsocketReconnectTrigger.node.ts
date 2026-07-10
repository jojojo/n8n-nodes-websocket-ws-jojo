import {
	ICredentialDataDecryptedObject,
	INodeType,
	INodeTypeDescription,
	ITriggerFunctions,
	ITriggerResponse,
	NodeOperationError,
} from 'n8n-workflow';

import WebSocket from 'ws';
import https from 'https';
import http from 'http';

function buildMultipart(fields: Array<{ name: string; value: string }>): { body: Buffer; boundary: string } {
	const boundary = `----FormBoundary${Date.now()}`;
	const parts: Buffer[] = [];
	for (const field of fields) {
		parts.push(Buffer.from(
			`--${boundary}\r\n` +
			`Content-Disposition: form-data; name="${field.name}"\r\n` +
			`\r\n` +
			`${field.value}\r\n`,
		));
	}
	parts.push(Buffer.from(`--${boundary}--\r\n`));
	return { body: Buffer.concat(parts), boundary };
}

function doRequest(
	url: string,
	method: string,
	headers: Record<string, string>,
	body?: Buffer | string,
): Promise<unknown> {
	return new Promise((resolve, reject) => {
		const isHttps = url.startsWith('https://');
		const client = isHttps ? https : http;
		const options: https.RequestOptions = {
			method,
			headers,
			...(isHttps ? { rejectUnauthorized: false } : {}),
		};
		const req = client.request(url, options, (res) => {
			let raw = '';
			res.on('data', (chunk: Buffer) => { raw += chunk.toString(); });
			res.on('end', () => {
				const status = res.statusCode ?? 0;
				if (status < 200 || status >= 300) {
					reject(new Error(`Token refresh HTTP ${status}: ${raw}`));
					return;
				}
				try { resolve(JSON.parse(raw)); }
				catch { resolve(raw); }
			});
		});
		req.on('error', reject);
		if (body) req.write(body);
		req.end();
	});
}

function extractByPath(data: unknown, path: string): string {
	if (!path) return String(data);
	const parts = path.split('.');
	let value: unknown = data;
	for (const part of parts) {
		value = (value as Record<string, unknown>)?.[part];
	}
	return String(value);
}

type TokenInjectionMethod = 'authorizationHeader' | 'queryParameter' | 'customHeader';

function injectTokenIntoConnection(params: {
	injectionMethod: TokenInjectionMethod;
	token: string;
	headers: Record<string, string>;
	url: string;
	prefix: string;
	parameterName: string;
	headerName: string;
}): { headers: Record<string, string>; url: string; injectedValue: string } {
	const {
		injectionMethod,
		token,
		headers,
		url,
		prefix,
		parameterName,
		headerName,
	} = params;

	const normalizedToken = token.trim();
	const normalizedPrefix = prefix || '';
	const injectedValue = normalizedPrefix && normalizedToken.startsWith(normalizedPrefix)
		? normalizedToken
		: `${normalizedPrefix}${normalizedToken}`;
	const nextHeaders = { ...headers };
	let nextUrl = url;

	if (injectionMethod === 'authorizationHeader') {
		nextHeaders.Authorization = injectedValue;
	} else if (injectionMethod === 'customHeader') {
		nextHeaders[headerName || 'X-API-Key'] = injectedValue;
	} else {
		const sep = nextUrl.includes('?') ? '&' : '?';
		nextUrl = `${nextUrl}${sep}${encodeURIComponent(parameterName || 'Authorization')}=${encodeURIComponent(injectedValue)}`;
	}

	return { headers: nextHeaders, url: nextUrl, injectedValue };
}

export class WebsocketReconnectTrigger implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Websocket Reconnect - Trigger',
		name: 'websocketReconnectTrigger',
		icon: 'file:websocket.svg',
		group: ['trigger'],
		version: 1,
		description: 'Connect to a WebSocket endpoint and trigger on incoming messages. Supports header/query auth, extra query parameters, periodic token refresh, and auto-reconnect.',
		defaults: {
			name: 'WebSocket Reconnect',
		},
		inputs: [],
		outputs: ['main'],
		credentials: [
			{
				name: 'websocketHeaderAuthApi',
				required: true,
				displayOptions: { show: { authentication: ['headerAuth'] } },
			},
			{
				name: 'websocketQueryAuthApi',
				required: true,
				displayOptions: { show: { authentication: ['queryAuth'] } },
			},
		],
		properties: [

			// ═══════════════════════════════════════════════════════════
			// CONNECTION
			// ═══════════════════════════════════════════════════════════
			{
				displayName: 'WebSocket URL',
				name: 'websocketUrl',
				type: 'string',
				default: '',
				placeholder: 'wss://example.com/ws?fmt=json',
				description: 'Full WebSocket URL. Static query params (e.g. fmt=JSON) can be included here.',
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
				description: 'Use credentials for auth. Choose None if you handle auth via Token Refresh or Query Parameters below.',
			},

			// ═══════════════════════════════════════════════════════════
			// EXTRA QUERY PARAMETERS
			// ═══════════════════════════════════════════════════════════
			{
				displayName: 'Extra Query Parameters',
				name: 'queryParameters',
				type: 'fixedCollection',
				typeOptions: { multipleValues: true },
				placeholder: 'Add Parameter',
				default: {},
				description: 'Static query parameters appended to the WebSocket URL on every connection',
				options: [
					{
						name: 'parameters',
						displayName: 'Parameter',
						values: [
							{
								displayName: 'Name',
								name: 'name',
								type: 'string',
								default: '',
								description: 'Parameter name',
							},
							{
								displayName: 'Value',
								name: 'value',
								type: 'string',
								default: '',
								description: 'Parameter value (do not URL-encode — the node handles that)',
							},
						],
					},
				],
			},

			// ═══════════════════════════════════════════════════════════
			// TOKEN REFRESH
			// ═══════════════════════════════════════════════════════════
			{
				displayName: 'Enable Token Refresh',
				name: 'tokenRefreshEnabled',
				type: 'boolean',
				default: false,
				description: 'Whether to fetch a fresh bearer token from an HTTP endpoint before connecting',
			},

			// — Refresh mode selector ————————————————————————————————————
			{
				displayName: 'Refresh Method',
				name: 'tokenRefreshMode',
				type: 'options',
				options: [
					{ name: 'Direct (Single Endpoint Returns Token)', value: 'direct' },
					{ name: 'Two-Step (Fetch Current Token → Call Refresh Endpoint)', value: 'twoStep' },
				],
				default: 'direct',
				displayOptions: { show: { tokenRefreshEnabled: [true] } },
				description: 'How to obtain a fresh token. Direct: one request returns the token. Two-Step: first fetch the current token, then call a refresh endpoint using it.',
			},

			// ── DIRECT MODE (existing) ──────────────────────────────────
			{
				displayName: 'Token Refresh URL',
				name: 'tokenRefreshUrl',
				type: 'string',
				default: '',
				placeholder: 'https://api.example.com/auth/token',
				displayOptions: { show: { tokenRefreshEnabled: [true], tokenRefreshMode: ['direct'] } },
				description: 'HTTP endpoint that returns a fresh token',
			},
			{
				displayName: 'HTTP Method',
				name: 'tokenRefreshMethod',
				type: 'options',
				options: [
					{ name: 'POST', value: 'POST' },
					{ name: 'GET', value: 'GET' },
				],
				default: 'POST',
				displayOptions: { show: { tokenRefreshEnabled: [true], tokenRefreshMode: ['direct'] } },
				description: 'HTTP method for the token refresh request',
			},
			{
				displayName: 'Request Headers',
				name: 'tokenRefreshHeaders',
				type: 'fixedCollection',
				typeOptions: { multipleValues: true },
				placeholder: 'Add Header',
				default: {},
				displayOptions: { show: { tokenRefreshEnabled: [true], tokenRefreshMode: ['direct'] } },
				description: 'Headers for the token refresh request (e.g. X-Admin-Token)',
				options: [
					{
						name: 'headers',
						displayName: 'Header',
						values: [
							{
								displayName: 'Name',
								name: 'name',
								type: 'string',
								default: '',
								placeholder: 'X-Admin-Token',
								description: 'Header name',
							},
							{
								displayName: 'Value',
								name: 'value',
								type: 'string',
								typeOptions: { password: true },
								default: '',
								description: 'Header value',
							},
						],
					},
				],
			},
			{
				displayName: 'Body Type',
				name: 'tokenRefreshBodyType',
				type: 'options',
				options: [
					{ name: 'Form URL Encoded (Application/X-Www-Form-Urlencoded)', value: 'formUrlEncoded' },
					{ name: 'Form Data (Multipart/form-Data)', value: 'formData' },
					{ name: 'JSON', value: 'json' },
					{ name: 'No Body', value: 'none' },
				],
				default: 'formData',
				displayOptions: { show: { tokenRefreshEnabled: [true], tokenRefreshMode: ['direct'], tokenRefreshMethod: ['POST'] } },
				description: 'Format of the request body',
			},
			{
				displayName: 'Form Fields',
				name: 'tokenRefreshFormFields',
				type: 'fixedCollection',
				typeOptions: { multipleValues: true },
				placeholder: 'Add Field',
				default: {},
				displayOptions: { show: { tokenRefreshEnabled: [true], tokenRefreshMode: ['direct'], tokenRefreshMethod: ['POST'], tokenRefreshBodyType: ['formData'] } },
				description: 'Form fields to send (e.g. tipo=Websockets, ttl_hours=12)',
				options: [
					{
						name: 'fields',
						displayName: 'Field',
						values: [
							{
								displayName: 'Name',
								name: 'name',
								type: 'string',
								default: '',
								description: 'Field name',
							},
							{
								displayName: 'Value',
								name: 'value',
								type: 'string',
								default: '',
								description: 'Field value',
							},
						],
					},
				],
			},
			{
				displayName: 'JSON Body',
				name: 'tokenRefreshJsonBody',
				type: 'string',
				typeOptions: { rows: 3, password: true },
				default: '{}',
				displayOptions: { show: { tokenRefreshEnabled: [true], tokenRefreshMode: ['direct'], tokenRefreshMethod: ['POST'], tokenRefreshBodyType: ['json'] } },
				description: 'JSON body for the token refresh POST request',
			},
			{
				displayName: 'Token Field in Response',
				name: 'tokenRefreshResponsePath',
				type: 'string',
				default: '',
				placeholder: 'token   or   data.access_token',
				displayOptions: { show: { tokenRefreshEnabled: [true], tokenRefreshMode: ['direct'] } },
				description: 'Dot-notation path to the token inside the JSON response. Example: if response is {"data":{"token":"abc"}}, enter data.token.',
			},

			// ── TWO-STEP MODE (new) ─────────────────────────────────────
			// Step 1: GET current token
			{
				displayName: 'Step 1 - Fetch Current Token URL',
				name: 'tsStep1Url',
				type: 'string',
				default: '',
				placeholder: 'https://api.example.com/api/token/latest',
				displayOptions: { show: { tokenRefreshEnabled: [true], tokenRefreshMode: ['twoStep'] } },
				description: 'GET endpoint that returns the current active bearer token',
			},
			{
				displayName: 'Step 1 - Admin Header Name',
				name: 'tsStep1AdminHeaderName',
				type: 'string',
				default: 'x-Admin',
				placeholder: 'x-Admin',
				displayOptions: { show: { tokenRefreshEnabled: [true], tokenRefreshMode: ['twoStep'] } },
				description: 'Name of the admin authentication header for the fetch request',
			},
			{
				displayName: 'Step 1 - Admin Header Value',
				name: 'tsStep1AdminHeaderValue',
				type: 'string',
				typeOptions: { password: true },
				default: '',
				placeholder: 'your-admin-key',
				displayOptions: { show: { tokenRefreshEnabled: [true], tokenRefreshMode: ['twoStep'] } },
				description: 'Value of the admin header (kept secret)',
			},
			{
				displayName: 'Step 1 - Token Field in Response',
				name: 'tsStep1ResponsePath',
				type: 'string',
				default: '',
				placeholder: 'token   or   data.bearer',
				displayOptions: { show: { tokenRefreshEnabled: [true], tokenRefreshMode: ['twoStep'] } },
				description: 'Dot-notation path to the current token in the step 1 response',
			},
			// Step 2: Refresh using current token
			{
				displayName: 'Step 2 - Refresh URL',
				name: 'tsStep2Url',
				type: 'string',
				default: '',
				placeholder: 'https://api.example.com/api/token/refresh',
				displayOptions: { show: { tokenRefreshEnabled: [true], tokenRefreshMode: ['twoStep'] } },
				description: 'POST endpoint that refreshes the token. The current token from step 1 is sent as Authorization: Bearer &lt;token&gt;.',
			},
			{
				displayName: 'Step 2 - New Token Field in Response',
				name: 'tsStep2ResponsePath',
				type: 'string',
				default: '',
				placeholder: 'token   or   data.new_token',
				displayOptions: { show: { tokenRefreshEnabled: [true], tokenRefreshMode: ['twoStep'] } },
				description: 'Dot-notation path to the new token in the step 2 response',
			},

			// ── SHARED (both modes) ─────────────────────────────────────
			{
				displayName: 'Token Injection Method',
				name: 'tokenInjectionMethod',
				type: 'options',
				options: [
					{ name: 'Authorization Header', value: 'authorizationHeader' },
					{ name: 'Query Parameter', value: 'queryParameter' },
					{ name: 'Custom Header', value: 'customHeader' },
				],
				default: 'queryParameter',
				displayOptions: { show: { tokenRefreshEnabled: [true] } },
				description: 'How to inject the refreshed token into the WebSocket handshake',
			},
			{
				displayName: 'WebSocket Query Param to Inject',
				name: 'tokenRefreshTargetParam',
				type: 'string',
				default: 'Authorization',
				placeholder: 'Authorization',
				displayOptions: { show: { tokenRefreshEnabled: [true], tokenInjectionMethod: ['queryParameter'] } },
				description: 'Name of the query parameter that will receive the fresh token',
			},
			{
				displayName: 'Header Name',
				name: 'tokenRefreshHeaderName',
				type: 'string',
				default: 'X-API-Key',
				placeholder: 'X-API-Key',
				displayOptions: { show: { tokenRefreshEnabled: [true], tokenInjectionMethod: ['customHeader'] } },
				description: 'Header name used when Token Injection Method is set to Custom Header',
			},
			{
				displayName: 'Token Prefix',
				name: 'tokenRefreshValuePrefix',
				type: 'string',
				default: 'Bearer ',
				placeholder: 'Bearer ',
				displayOptions: { show: { tokenRefreshEnabled: [true] } },
				description: 'Text prepended to the token value (include trailing space if needed, e.g. "Bearer ")',
			},

			// — Periodic refresh ———————————————————————————————————————
			{
				displayName: 'Refresh Token Periodically',
				name: 'tokenRefreshOnSchedule',
				type: 'boolean',
				default: true,
				displayOptions: { show: { tokenRefreshEnabled: [true] } },
				description: 'Whether to automatically refresh the token and reconnect at a set interval (recommended when token has an expiry)',
			},
			{
				displayName: 'Refresh Interval',
				name: 'tokenRefreshIntervalHours',
				type: 'number',
				default: 11,
				displayOptions: { show: { tokenRefreshEnabled: [true], tokenRefreshOnSchedule: [true] } },
				description: 'How often to refresh the token and reconnect. Unit is set by the field below.',
			},
			{
				displayName: 'Refresh Interval Unit',
				name: 'tokenRefreshIntervalUnit',
				type: 'options',
				options: [
					{ name: 'Hours', value: 'hours' },
					{ name: 'Minutes', value: 'minutes' },
					{ name: 'Seconds', value: 'seconds' },
				],
				default: 'hours',
				displayOptions: { show: { tokenRefreshEnabled: [true], tokenRefreshOnSchedule: [true] } },
				description: 'Unit for the refresh interval above',
			},

			// ═══════════════════════════════════════════════════════════
			// AUTO RECONNECT (on unexpected drops)
			// ═══════════════════════════════════════════════════════════
			{
				displayName: 'Auto Reconnect on Drop',
				name: 'autoReconnect',
				type: 'boolean',
				default: true,
				description: 'Whether to automatically reconnect if the connection drops unexpectedly',
			},
			{
				displayName: 'Reconnect Interval (Seconds)',
				name: 'reconnectInterval',
				type: 'number',
				default: 10,
				displayOptions: { show: { autoReconnect: [true] } },
				description: 'Seconds to wait before attempting reconnection after an unexpected drop',
			},
			{
				displayName: 'Max Reconnect Attempts',
				name: 'maxReconnectAttempts',
				type: 'number',
				default: 0,
				displayOptions: { show: { autoReconnect: [true] } },
				description: 'Maximum reconnection attempts after unexpected drop (0 = unlimited)',
			},

			// ═══════════════════════════════════════════════════════════
			// INITIAL MESSAGE
			// ═══════════════════════════════════════════════════════════
			{
				displayName: 'Send Initial Message',
				name: 'sendInitMessage',
				type: 'boolean',
				default: false,
				description: 'Whether to send a message immediately after connecting',
			},
			{
				displayName: 'Initial Message',
				name: 'initMessage',
				type: 'string',
				displayOptions: { show: { sendInitMessage: [true] } },
				required: true,
				default: '{}',
				description: 'Message to send upon connecting (JSON or plain text)',
			},

			// ═══════════════════════════════════════════════════════════
			// OUTPUT
			// ═══════════════════════════════════════════════════════════
			{
				displayName: 'Return WS Resource',
				name: 'returnWs',
				type: 'boolean',
				default: false,
				description: 'Whether to include the raw ws object in output (needed to send reply messages)',
			},
		],
	};

	async trigger(this: ITriggerFunctions): Promise<ITriggerResponse> {
		let ws: WebSocket | null = null;
		let reconnectAttempts = 0;
		let isClosed = false;
		let refreshTimer: ReturnType<typeof setTimeout> | null = null;
		let currentBearerToken = '';
		let tokenRefreshInFlight: Promise<string> | null = null;
		let connectionFlowInFlight: Promise<void> | null = null;

		const websocketUrl           = this.getNodeParameter('websocketUrl', '') as string;
		const authentication         = this.getNodeParameter('authentication', 'none') as string;
		const sendInitMessage        = this.getNodeParameter('sendInitMessage', false) as boolean;
		const returnWs               = this.getNodeParameter('returnWs', false) as boolean;
		const autoReconnect          = this.getNodeParameter('autoReconnect', true) as boolean;
		const reconnectInterval      = (this.getNodeParameter('reconnectInterval', 10) as number) * 1000;
		const maxReconnects          = this.getNodeParameter('maxReconnectAttempts', 0) as number;
		const tokenRefreshEnabled    = this.getNodeParameter('tokenRefreshEnabled') as boolean;
		const tokenRefreshOnSchedule = tokenRefreshEnabled && (this.getNodeParameter('tokenRefreshOnSchedule', false) as boolean);
		const tokenRefreshIntervalValue = this.getNodeParameter('tokenRefreshIntervalHours', 11) as number;
		const tokenRefreshIntervalUnit  = this.getNodeParameter('tokenRefreshIntervalUnit', 'hours') as string;
		const unitMs = tokenRefreshIntervalUnit === 'seconds' ? 1000 : tokenRefreshIntervalUnit === 'minutes' ? 60_000 : 3_600_000;
		const tokenRefreshIntervalMs = tokenRefreshIntervalValue * unitMs;

		// Fetch step-1 token only (GET latest active token) — used for initial connect and reconnect-on-drop
		const fetchLatestToken = async (): Promise<string> => {
			const refreshMode = this.getNodeParameter('tokenRefreshMode', 'direct') as string;

			if (refreshMode === 'twoStep') {
				const step1Url              = this.getNodeParameter('tsStep1Url', '') as string;
				const step1AdminHeaderName  = this.getNodeParameter('tsStep1AdminHeaderName', 'x-Admin') as string;
				const step1AdminHeaderValue = this.getNodeParameter('tsStep1AdminHeaderValue', '') as string;
				const step1ResponsePath     = this.getNodeParameter('tsStep1ResponsePath', '') as string;

				const step1Headers: Record<string, string> = {
					'accept': 'application/json',
					[step1AdminHeaderName]: step1AdminHeaderValue,
				};
				const step1Data = await doRequest(step1Url, 'GET', step1Headers);
				console.debug('[websocket-ws] two-step step1 (latest) response:', JSON.stringify(step1Data));

				const currentToken = extractByPath(step1Data, step1ResponsePath);
				if (!currentToken || currentToken === 'undefined' || currentToken === 'null') {
					throw new NodeOperationError(
						this.getNode(),
						`Two-step token (step 1): could not find field "${step1ResponsePath}" in response. Full response: ${JSON.stringify(step1Data)}`,
					);
				}
				return currentToken;
			}

			// direct mode: same single-endpoint fetch
			return fetchFreshToken();
		};

		const fetchFreshToken = async (): Promise<string> => {
			const refreshMode   = this.getNodeParameter('tokenRefreshMode', 'direct') as string;

			// ── TWO-STEP MODE ────────────────────────────────────────────
			if (refreshMode === 'twoStep') {
				const step1Url             = this.getNodeParameter('tsStep1Url', '') as string;
				const step1AdminHeaderName = this.getNodeParameter('tsStep1AdminHeaderName', 'x-Admin') as string;
				const step1AdminHeaderValue= this.getNodeParameter('tsStep1AdminHeaderValue', '') as string;
				const step1ResponsePath    = this.getNodeParameter('tsStep1ResponsePath', '') as string;

				// Step 1: GET current token using admin header
				const step1Headers: Record<string, string> = {
					'accept': 'application/json',
					[step1AdminHeaderName]: step1AdminHeaderValue,
				};
				const step1Data = await doRequest(step1Url, 'GET', step1Headers);
				console.debug('[websocket-ws] two-step step1 response:', JSON.stringify(step1Data));

				const currentToken = extractByPath(step1Data, step1ResponsePath);
				if (!currentToken || currentToken === 'undefined' || currentToken === 'null') {
					throw new NodeOperationError(
						this.getNode(),
						`Two-step token refresh (step 1): could not find field "${step1ResponsePath}" in response. Full response: ${JSON.stringify(step1Data)}`,
					);
				}

				// Step 2: POST to refresh endpoint, passing current token as Bearer
				const step2Url          = this.getNodeParameter('tsStep2Url', '') as string;
				const step2ResponsePath = this.getNodeParameter('tsStep2ResponsePath', '') as string;

				const step2Headers: Record<string, string> = {
					'accept': 'application/json',
					'Authorization': `Bearer ${currentToken}`,
				};
				const step2Data = await doRequest(step2Url, 'POST', step2Headers);
				console.debug('[websocket-ws] two-step step2 response:', JSON.stringify(step2Data));

				const newToken = extractByPath(step2Data, step2ResponsePath);
				if (!newToken || newToken === 'undefined' || newToken === 'null') {
					throw new NodeOperationError(
						this.getNode(),
						`Two-step token refresh (step 2): could not find field "${step2ResponsePath}" in response. Full response: ${JSON.stringify(step2Data)}`,
					);
				}
				return newToken;
			}

			// ── DIRECT MODE ──────────────────────────────────────────────
			const refreshUrl    = this.getNodeParameter('tokenRefreshUrl', '') as string;
			const refreshMethod = this.getNodeParameter('tokenRefreshMethod', 'POST') as string;
			const responsePath  = this.getNodeParameter('tokenRefreshResponsePath', '') as string;

			const reqHeaders: Record<string, string> = { 'accept': 'application/json' };
			const customHeaders = this.getNodeParameter('tokenRefreshHeaders', {}) as {
				headers?: Array<{ name: string; value: string }>;
			};
			for (const h of customHeaders.headers || []) {
				if (h.name) reqHeaders[h.name] = h.value;
			}

			let requestBody: Buffer | string | undefined;
			if (refreshMethod === 'POST') {
				const bodyType = this.getNodeParameter('tokenRefreshBodyType', 'formData') as string;
				if (bodyType === 'formUrlEncoded') {
					const formFieldsParam = this.getNodeParameter('tokenRefreshFormFields', {}) as {
						fields?: Array<{ name: string; value: string }>;
					};
					const urlEncodedBody = new URLSearchParams();
					for (const field of formFieldsParam.fields || []) {
						if (field.name) {
							urlEncodedBody.append(field.name, field.value ?? '');
						}
					}
					const bodyString = urlEncodedBody.toString();
					reqHeaders['Content-Type'] = 'application/x-www-form-urlencoded';
					reqHeaders['Content-Length'] = String(Buffer.byteLength(bodyString));
					requestBody = bodyString;
				} else if (bodyType === 'formData') {
					const formFieldsParam = this.getNodeParameter('tokenRefreshFormFields', {}) as {
						fields?: Array<{ name: string; value: string }>;
					};
					const { body: multipartBody, boundary } = buildMultipart(formFieldsParam.fields || []);
					reqHeaders['Content-Type'] = `multipart/form-data; boundary=${boundary}`;
					reqHeaders['Content-Length'] = String(multipartBody.length);
					requestBody = multipartBody;
				} else if (bodyType === 'json') {
					const jsonBody = this.getNodeParameter('tokenRefreshJsonBody', '{}') as string;
					reqHeaders['Content-Type'] = 'application/json';
					reqHeaders['Content-Length'] = String(Buffer.byteLength(jsonBody));
					requestBody = jsonBody;
				}
			}

			const data = await doRequest(refreshUrl, refreshMethod, reqHeaders, requestBody);
			console.debug('[websocket-ws] token refresh response:', JSON.stringify(data));
			const rawToken = extractByPath(data, responsePath);
			if (!rawToken || rawToken === 'undefined' || rawToken === 'null') {
				throw new NodeOperationError(
					this.getNode(),
					`Token refresh: could not find field "${responsePath}" in response. Full response: ${JSON.stringify(data)}`,
				);
			}
			return rawToken;
		};

		const getTokenForConnection = async (useFullRefresh = false): Promise<string> => {
			if (tokenRefreshInFlight) {
				return tokenRefreshInFlight;
			}

			tokenRefreshInFlight = (async () => {
				const token = useFullRefresh ? await fetchFreshToken() : await fetchLatestToken();
				if (!token || !token.trim()) {
					throw new NodeOperationError(this.getNode(), 'Token refresh returned an empty token, aborting WebSocket handshake.');
				}
				return token;
			})();

			try {
				return await tokenRefreshInFlight;
			} finally {
				tokenRefreshInFlight = null;
			}
		};

		const runExclusiveConnectionFlow = async (flow: () => Promise<void>): Promise<void> => {
			while (connectionFlowInFlight) {
				await connectionFlowInFlight;
			}

			const currentFlow = flow().finally(() => {
				if (connectionFlowInFlight === currentFlow) {
					connectionFlowInFlight = null;
				}
			});
			connectionFlowInFlight = currentFlow;
			await currentFlow;
		};

		const buildConnectionOptions = async (useFullRefresh = false): Promise<{ url: string; headers: Record<string, string> }> => {
			let url = websocketUrl;
			const headers: Record<string, string> = {};

			if (authentication === 'headerAuth') {
				const creds = await this.getCredentials('websocketHeaderAuthApi') as ICredentialDataDecryptedObject;
				headers[(creds.headerName as string) || 'Authorization'] = (creds.headerValue as string) || '';
			}

			if (authentication === 'queryAuth') {
				const creds = await this.getCredentials('websocketQueryAuthApi') as ICredentialDataDecryptedObject;
				const sep = url.includes('?') ? '&' : '?';
				url = `${url}${sep}${encodeURIComponent((creds.paramName as string) || 'token')}=${encodeURIComponent((creds.paramValue as string) || '')}`;
			}

			const queryParameters = this.getNodeParameter('queryParameters', {}) as {
				parameters?: Array<{ name: string; value: string }>;
			};
			for (const param of queryParameters.parameters || []) {
				if (param.name) {
					const sep = url.includes('?') ? '&' : '?';
					url = `${url}${sep}${encodeURIComponent(param.name)}=${encodeURIComponent(param.value)}`;
				}
			}

			if (tokenRefreshEnabled) {
				const tokenInjectionMethod = this.getNodeParameter('tokenInjectionMethod', 'queryParameter') as TokenInjectionMethod;
				const targetParam = this.getNodeParameter('tokenRefreshTargetParam', 'Authorization') as string;
				const headerName = this.getNodeParameter('tokenRefreshHeaderName', 'X-API-Key') as string;
				const tokenPrefix = this.getNodeParameter('tokenRefreshValuePrefix', 'Bearer ') as string;
				const freshToken = await getTokenForConnection(useFullRefresh);
				const injected = injectTokenIntoConnection({
					injectionMethod: tokenInjectionMethod,
					token: freshToken,
					headers,
					url,
					prefix: tokenPrefix,
					parameterName: targetParam,
					headerName,
				});
				for (const [key, value] of Object.entries(injected.headers)) {
					headers[key] = value;
				}
				url = injected.url;
				currentBearerToken = injected.injectedValue;
				const injectionTarget = tokenInjectionMethod === 'queryParameter'
					? `query:${targetParam}`
					: tokenInjectionMethod === 'customHeader'
						? `header:${headerName || 'X-API-Key'}`
						: 'header:Authorization';
				console.debug('[websocket-ws] token refreshed, injected into', injectionTarget);
			}

			return { url, headers };
		};

		// Per-instance set: WS instances that should close silently (no event, no reconnect)
		const silentCloseInstances = new Set<WebSocket>();

		const emitTokenRefreshed = () => {
			if (!currentBearerToken) return;
			this.emit([this.helpers.returnJsonArray([{
				event: 'tokenRefreshed',
				bearerToken: currentBearerToken,
				expiresIn: this.getNodeParameter('tokenRefreshIntervalHours', 11),
				expiresInUnit: this.getNodeParameter('tokenRefreshIntervalUnit', 'hours'),
			}])]);
		};

		const setupWsEvents = (wsInstance: WebSocket) => {
			wsInstance.on('error', (error: Error) => {
				console.warn('[websocket-ws] connection error:', error.message);
				this.emit([this.helpers.returnJsonArray([{ event: 'error', message: error.message }])]);
			});

			wsInstance.on('close', () => {
				if (silentCloseInstances.has(wsInstance)) { silentCloseInstances.delete(wsInstance); return; }

				console.debug('[websocket-ws] connection closed');
				this.emit([this.helpers.returnJsonArray([{ event: 'close' }])]);
				if (refreshTimer) clearTimeout(refreshTimer);
				if (isClosed) return;

				if (autoReconnect) {
					const canRetry = maxReconnects === 0 || reconnectAttempts < maxReconnects;
					if (canRetry) {
						reconnectAttempts++;
						console.debug(`[websocket-ws] reconnecting in ${reconnectInterval / 1000}s (attempt ${reconnectAttempts})`);
						setTimeout(() => { startConsumer(); }, reconnectInterval);
					} else {
						console.warn(`[websocket-ws] max reconnect attempts (${maxReconnects}) reached`);
					}
				}
			});

			wsInstance.on('message', (data: Buffer | string, isBinary: boolean) => {
				let message: any = isBinary ? data : data.toString();
				try { message = JSON.parse(message as string); }
				catch { /* not JSON */ }
				reconnectAttempts = 0;
				this.emit([this.helpers.returnJsonArray([{ event: 'message', message, ws: returnWs ? ws : null }])]);
			});

			wsInstance.on('open', () => {
				console.debug('[websocket-ws] connected');
				reconnectAttempts = 0;
				if (sendInitMessage) {
					wsInstance.send(this.getNodeParameter('initMessage', '') as string);
				}
				this.emit([this.helpers.returnJsonArray([{
					event: 'open',
					bearerToken: currentBearerToken || null,
					ws: returnWs ? wsInstance : null,
				}])]);
				emitTokenRefreshed();
				scheduleTokenRefresh();
			});
		};

		// Overlap refresh: connect new WS before closing old one
		const doOverlapRefresh = async () => {
			await runExclusiveConnectionFlow(async () => {
				console.debug('[websocket-ws] overlap token refresh starting');
				// Mark current WS as silently-closeable NOW — server may close it as soon
				// as the new connection arrives, before newWs fires 'open'.
				const outgoingWs = ws;
				if (outgoingWs) silentCloseInstances.add(outgoingWs);

				try {
					const { url, headers } = await buildConnectionOptions(true);
					const newWs = new WebSocket(url, { headers });

					newWs.once('error', (error: Error) => {
						console.warn('[websocket-ws] overlap new connection failed, keeping old:', error.message);
						// New connection failed — remove silent mark so old WS close still triggers reconnect
						if (outgoingWs) silentCloseInstances.delete(outgoingWs);
						newWs.terminate();
						scheduleTokenRefresh(); // retry after interval
					});

					newWs.once('open', () => {
						console.debug('[websocket-ws] overlap new connection open, closing old');
						ws = newWs;
						setupWsEvents(newWs);
						if (outgoingWs) {
							silentCloseInstances.add(outgoingWs); // ensure still marked
							outgoingWs.terminate();
						}
						reconnectAttempts = 0;
						if (sendInitMessage) {
							newWs.send(this.getNodeParameter('initMessage', '') as string);
						}
						this.emit([this.helpers.returnJsonArray([{
							event: 'open',
							bearerToken: currentBearerToken || null,
							ws: returnWs ? newWs : null,
						}])]);
						emitTokenRefreshed();
						scheduleTokenRefresh();
					});
				} catch (error: unknown) {
					if (outgoingWs) silentCloseInstances.delete(outgoingWs);
					const msg = error instanceof Error ? error.message : String(error);
					console.warn('[websocket-ws] overlap refresh error:', msg);
					scheduleTokenRefresh();
				}
			});
		};

		const scheduleTokenRefresh = () => {
			if (refreshTimer) clearTimeout(refreshTimer);
			if (!tokenRefreshOnSchedule || isClosed) return;
			refreshTimer = setTimeout(() => {
				if (!isClosed) doOverlapRefresh();
			}, tokenRefreshIntervalMs);
		};

		const startConsumer = async (): Promise<void> => {
			await runExclusiveConnectionFlow(async () => {
				try {
					const { url, headers } = await buildConnectionOptions();
					ws = new WebSocket(url, { headers });
					setupWsEvents(ws);
				} catch (error: unknown) {
					const msg = error instanceof Error ? error.message : String(error);
					throw new NodeOperationError(this.getNode(), `Execution error: ${msg}`);
				}
			});
		};

		await startConsumer();

		async function closeFunction() {
			isClosed = true;
			if (refreshTimer) clearTimeout(refreshTimer);
			if (ws) ws.terminate();
		}

		async function manualTriggerFunction() {
			await startConsumer();
		}

		return { closeFunction, manualTriggerFunction };
	}
}
