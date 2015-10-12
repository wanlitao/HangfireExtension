CREATE TABLE [$(HangFireSchema).Schema] (
        [Version]       integer NOT NULL

);

CREATE TABLE [$(HangFireSchema).Lock] (
        [Id]    INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
		[Resource] nvarchar(100) NOT NULL COLLATE NOCASE
);
CREATE UNIQUE INDEX [Lock_IX_HangFire_Lock_Resource]
ON [$(HangFireSchema).Lock]
([Resource]);

CREATE TABLE [$(HangFireSchema).Job] (
        [Id]    INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        [StateId]       integer,
        [StateName]     nvarchar(20) COLLATE NOCASE,
        [InvocationData] nvarchar NOT NULL COLLATE NOCASE,
        [Arguments]     nvarchar NOT NULL COLLATE NOCASE,
        [CreatedAt]     datetime NOT NULL,
        [ExpireAt]      datetime

);
CREATE INDEX [Job_IX_HangFire_Job_ExpireAt]
ON [$(HangFireSchema).Job]
([ExpireAt] DESC);
CREATE INDEX [Job_IX_HangFire_Job_StateName]
ON [$(HangFireSchema).Job]
([StateName] DESC);

CREATE TABLE [$(HangFireSchema).State] (
        [Id]    INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        [JobId] integer NOT NULL,
        [Name]  nvarchar(20) NOT NULL COLLATE NOCASE,
        [Reason]        nvarchar(100) COLLATE NOCASE,
        [CreatedAt]     datetime NOT NULL,
        [Data]  nvarchar COLLATE NOCASE
,
    FOREIGN KEY ([JobId])
        REFERENCES [$(HangFireSchema).Job]([Id])
);
CREATE INDEX [State_IX_HangFire_State_JobId]
ON [$(HangFireSchema).State]
([JobId] DESC);

CREATE TABLE [$(HangFireSchema).JobParameter] (
        [Id]    INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        [JobId] integer NOT NULL,
        [Name]  nvarchar(40) NOT NULL COLLATE NOCASE,
        [Value] nvarchar COLLATE NOCASE
,
    FOREIGN KEY ([JobId])
        REFERENCES [$(HangFireSchema).Job]([Id])
);
CREATE INDEX [JobParameter_IX_HangFire_JobParameter_JobIdAndName]
ON [$(HangFireSchema).JobParameter]
([JobId] DESC, [Name] DESC);

CREATE TABLE [$(HangFireSchema).JobQueue] (
        [Id]    INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        [JobId] integer NOT NULL,
        [Queue] nvarchar(20) NOT NULL COLLATE NOCASE,
        [FetchedAt]     datetime

);
CREATE INDEX [JobQueue_IX_HangFire_JobQueue_QueueAndFetchedAt]
ON [$(HangFireSchema).JobQueue]
([Queue] DESC, [FetchedAt] DESC);

CREATE TABLE [$(HangFireSchema).Server] (
        [Id]    nvarchar(50) PRIMARY KEY NOT NULL COLLATE NOCASE,
        [Data]  nvarchar COLLATE NOCASE,
        [LastHeartbeat] datetime NOT NULL

);

CREATE TABLE [$(HangFireSchema).List] (
        [Id]    INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        [Key]   nvarchar(100) NOT NULL COLLATE NOCASE,
        [Value] nvarchar COLLATE NOCASE,
        [ExpireAt]      datetime

);
CREATE INDEX [List_IX_HangFire_List_ExpireAt]
ON [$(HangFireSchema).List]
([ExpireAt] DESC);
CREATE INDEX [List_IX_HangFire_List_Key]
ON [$(HangFireSchema).List]
([Key] DESC);

CREATE TABLE [$(HangFireSchema).Set] (
        [Id]    INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        [Key]   nvarchar(100) NOT NULL COLLATE NOCASE,
        [Score] float NOT NULL,
        [Value] nvarchar(256) NOT NULL COLLATE NOCASE,
        [ExpireAt]      datetime

);
CREATE INDEX [Set_IX_HangFire_Set_ExpireAt]
ON [$(HangFireSchema).Set]
([ExpireAt] DESC);
CREATE INDEX [Set_IX_HangFire_Set_Key]
ON [$(HangFireSchema).Set]
([Key] DESC);
CREATE UNIQUE INDEX [Set_UX_HangFire_Set_KeyAndValue]
ON [$(HangFireSchema).Set]
([Key] DESC, [Value] DESC);

CREATE TABLE [$(HangFireSchema).Counter] (
        [Id]    INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        [Key]   nvarchar(100) NOT NULL COLLATE NOCASE,
        [Value] smallint NOT NULL,
        [ExpireAt]      datetime

);
CREATE INDEX [Counter_IX_HangFire_Counter_Key]
ON [$(HangFireSchema).Counter]
([Key] DESC);

CREATE TABLE [$(HangFireSchema).Hash] (
        [Id]    INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        [Key]   nvarchar(100) NOT NULL COLLATE NOCASE,
        [Field] nvarchar(100) NOT NULL COLLATE NOCASE,
        [Value] nvarchar COLLATE NOCASE,
        [ExpireAt]      datetime COLLATE NOCASE

);
CREATE INDEX [Hash_IX_HangFire_Hash_ExpireAt]
ON [$(HangFireSchema).Hash]
([ExpireAt] DESC);
CREATE INDEX [Hash_IX_HangFire_Hash_Key]
ON [$(HangFireSchema).Hash]
([Key] DESC);
CREATE UNIQUE INDEX [Hash_UX_HangFire_Hash_Key_Field]
ON [$(HangFireSchema).Hash]
([Key] DESC, [Field] DESC);

CREATE TABLE [$(HangFireSchema).AggregatedCounter] (
        [Id]    INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        [Key]   nvarchar(100) NOT NULL COLLATE NOCASE,
        [Value] integer NOT NULL,
        [ExpireAt]      datetime

);
CREATE UNIQUE INDEX [AggregatedCounter_UX_HangFire_CounterAggregated_Key]
ON [$(HangFireSchema).AggregatedCounter]
([Key] DESC);
