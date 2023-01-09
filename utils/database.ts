const mssql = require('mssql')
const TIMEOUT = 30 * 1000; // 30 secs
const sqlConfig = {
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    server: process.env.DB_SERVER,
    requestTimeout: TIMEOUT,
    connectionTimeout: TIMEOUT,
    pool: {
        max: 10,
        min: 0,
        idleTimeoutMillis: 60000
    },
    options: {
        requestTimeout: TIMEOUT,
        connectionTimeout: TIMEOUT,
        encrypt: false, // for azure
        trustServerCertificate: false // change to true for local dev / self-signed certs
    }
}

declare type SqlQueryResult = {
    rowsAffected: any[]
    returnValue: any;
    output: any;
    recordset: { [key: string]: any }[]
}
async function execute(sql: string): Promise<SqlQueryResult> {
    await mssql.connect(sqlConfig);
    return await mssql.query(sql);
}
class SqlRequestBuilder {
    constructor(private transction?: any){}
    private inputs: {paramName: string, typeOrValue: any, value?: any}[] = [];
    private outputs: {paramName: string, typeOrValue: any, value?: any}[] = [];

    public input(paramName: string, typeOrValue: any, value?: any): SqlRequestBuilder {
        this.inputs.push({paramName, typeOrValue, value})
        return this;
    }
    public numberInput(paramName: string, value: number): SqlRequestBuilder {
        this.inputs.push({paramName, typeOrValue: mssql.Int, value})
        return this;
    }
    public stringInput(paramName: string, value: string): SqlRequestBuilder {
        this.inputs.push({paramName, typeOrValue: mssql.NVarChar, value})
        return this;
    }
    public dateTimeInput(paramName: string, value: Date): SqlRequestBuilder {
        //@ts-ignore
        this.inputs.push({paramName, typeOrValue: mssql.DateTime, value})
        return this;
    }

    public output(paramName: string, typeOrValue: any, value?: any): SqlRequestBuilder {
        this.outputs.push({paramName, typeOrValue, value})
        return this;
    }

    public async execute(sql: string): Promise<SqlQueryResult> {
        if(!this.transction) await mssql.connect(sqlConfig);
        const request: SqlRequest = this.transction ? new mssql.Request(this.transction) : new mssql.Request();

        this.inputs.forEach(_in => request.input(_in.paramName, _in.typeOrValue, _in.value));
        this.outputs.forEach(_out => request.output(_out.paramName, _out.typeOrValue, _out.value));

        return request.query(sql);

    }
}

declare type SqlRequest = {
    input: (paramName: string, typeOrValue: any, value?: any) => void
    dateTimeInput: (paramName: string, value: Date) => void
    numberInput: (paramName: string, value: number) => void
    stringInput: (paramName: string, value: string) => void
    output: (paramName: string, typeOrValue: any, value?: any) => void
    query: (sql: string) => Promise<SqlQueryResult>
}

async function getSqlRequest(): Promise<SqlRequestBuilder> {
    return new SqlRequestBuilder();
}

async function beginTransaction(run: (getRequest: ()=> SqlRequestBuilder, abort: (reason?: string) => void) => Promise<any> | any) {
    await mssql.connect(sqlConfig);
    const transaction = new mssql.Transaction();
    return new Promise((t_resolve, t_reject) => {
        let _abortedFlag: string | undefined = undefined;
        let _requestCount = 0;
        function getRequest(): SqlRequestBuilder {
            if (_abortedFlag) throw new Error('Transaction already rolled back')
            _requestCount++;
            return new SqlRequestBuilder(transaction)
        }
        transaction.begin(async (_t_err: any) => {
            let rolledBack = false
            transaction.on('rollback', (aborted: boolean) => {
                // emited with aborted === true
                rolledBack = true
            })

    
            function abort(): Promise<void> {
                return new Promise((resolve, reject)=> {
                    if (!rolledBack) {
                        transaction.rollback((_err: any) => {
                            if(!_err) resolve();
                            else reject(_err)
                        })
                    } else resolve();
                });
            }

            function commit(): Promise<void> {
               return new Promise((c_resolve, c_reject) => {
                    if (!_requestCount) return c_reject('Nothing to commit')
                    transaction.commit((__err: any) => {
                       if (!__err) c_resolve();
                       else c_reject(__err);
                    })
                });
            }
    
            try {
                const result = await run(getRequest, (reason) => {
                    _abortedFlag = reason || '__ABORTED';
                });
                if(!_abortedFlag) {
                    await commit();
                    t_resolve(result);
                } else throw new Error(_abortedFlag);
            } catch (err: any) {
                try {
                    await abort()
                    t_reject(err);
                } catch (error) {
                    t_reject(error);
                }
            }
        })
    })
}

const MSSQLDB = {
    execute,
    getSqlRequest,
    beginTransaction
}

export default MSSQLDB;