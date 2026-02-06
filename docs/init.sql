create schema if not exists base;

create table base.account
(
    id         serial
        primary key,
    name       varchar(255)                        not null
        unique,
    modules    text[]    default '{}'::text[]      not null,
    enabled    boolean   default true              not null,
    config     jsonb     default '{}'::jsonb       not null,
    priority   integer   default 0                 not null,
    created_at timestamp default CURRENT_TIMESTAMP not null,
    updated_at timestamp default CURRENT_TIMESTAMP not null
);



create index idx_account_enabled
    on base.account (enabled);

create index idx_account_name
    on base.account (name);

create index idx_account_priority
    on base.account (priority);

create index idx_module_enabled
    on base.account (enabled);

create index idx_module_name
    on base.account (name);

create index idx_module_priority
    on base.account (priority);


create table base.data_middleware
(
    id         serial
        primary key,
    name       varchar(255)                        not null
        unique,
    weight     integer   default 0                 not null,
    enabled    boolean   default true              not null,
    config     jsonb     default '{}'::jsonb       not null,
    created_at timestamp default CURRENT_TIMESTAMP not null,
    updated_at timestamp default CURRENT_TIMESTAMP not null
);

create index idx_data_middleware_enabled
    on base.data_middleware (enabled);

create index idx_data_middleware_name
    on base.data_middleware (name);

create index idx_data_middleware_weight
    on base.data_middleware (weight);


create table base.download_middleware
(
    id         serial
        primary key,
    name       varchar(255)                        not null
        unique,
    weight     integer   default 0                 not null,
    enabled    boolean   default true              not null,
    config     jsonb     default '{}'::jsonb       not null,
    created_at timestamp default CURRENT_TIMESTAMP not null,
    updated_at timestamp default CURRENT_TIMESTAMP not null
);


create index idx_download_middleware_enabled
    on base.download_middleware (enabled);

create index idx_download_middleware_name
    on base.download_middleware (name);

create index idx_download_middleware_weight
    on base.download_middleware (weight);

create table base.module
(
    id         serial
        primary key,
    name       varchar(255)                        not null
        unique,
    enabled    boolean   default true              not null,
    config     jsonb     default '{}'::jsonb       not null,
    priority   integer   default 0                 not null,
    created_at timestamp default CURRENT_TIMESTAMP not null,
    updated_at timestamp default CURRENT_TIMESTAMP not null,
    version    integer   default 1                 not null
);


create table base.platform
(
    id          serial
        primary key,
    name        varchar(255)                        not null
        unique,
    description text,
    base_url    varchar(500),
    enabled     boolean   default true              not null,
    config      jsonb     default '{}'::jsonb       not null,
    created_at  timestamp default CURRENT_TIMESTAMP not null,
    updated_at  timestamp default CURRENT_TIMESTAMP not null
);

create index idx_platform_enabled
    on base.platform (enabled);

create index idx_platform_name
    on base.platform (name);

create table base.rel_account_platform
(
    account_id  integer                             not null
        references base.account
            on delete cascade,
    platform_id integer                             not null
        references base.platform
            on delete cascade,
    enabled     boolean   default true              not null,
    config      jsonb     default '{}'::jsonb       not null,
    created_at  timestamp default CURRENT_TIMESTAMP not null,
    updated_at  timestamp default CURRENT_TIMESTAMP not null,
    primary key (account_id, platform_id)
);


create index idx_rel_account_platform_account_enabled
    on base.rel_account_platform (account_id, enabled);

create index idx_rel_account_platform_enabled
    on base.rel_account_platform (enabled);

create index idx_rel_account_platform_platform_enabled
    on base.rel_account_platform (platform_id, enabled);

create table base.rel_module_account
(
    module_id  integer                             not null
        references base.module
            on delete cascade,
    account_id integer                             not null
        references base.account
            on delete cascade,
    priority   integer   default 0                 not null,
    enabled    boolean   default true              not null,
    config     jsonb     default '{}'::jsonb       not null,
    created_at timestamp default CURRENT_TIMESTAMP not null,
    updated_at timestamp default CURRENT_TIMESTAMP not null,
    primary key (module_id, account_id)
);


create table base.rel_module_data_middleware
(
    module_id          integer                             not null
        references base.module
            on delete cascade,
    data_middleware_id integer                             not null
        references base.data_middleware
            on delete cascade,
    priority           integer   default 0                 not null,
    enabled            boolean   default true              not null,
    config             jsonb     default '{}'::jsonb       not null,
    created_at         timestamp default CURRENT_TIMESTAMP not null,
    updated_at         timestamp default CURRENT_TIMESTAMP not null,
    primary key (module_id, data_middleware_id)
);

create table base.rel_module_download_middleware
(
    module_id              integer                             not null
        references base.module
            on delete cascade,
    download_middleware_id integer                             not null
        references base.download_middleware
            on delete cascade,
    priority               integer   default 0                 not null,
    enabled                boolean   default true              not null,
    config                 jsonb     default '{}'::jsonb       not null,
    created_at             timestamp default CURRENT_TIMESTAMP not null,
    updated_at             timestamp default CURRENT_TIMESTAMP not null,
    primary key (module_id, download_middleware_id)
);

create table base.rel_module_platform
(
    module_id   integer                             not null
        references base.module
            on delete cascade,
    platform_id integer                             not null
        references base.platform
            on delete cascade,
    priority    integer   default 0                 not null,
    enabled     boolean   default true              not null,
    config      jsonb     default '{}'::jsonb       not null,
    created_at  timestamp default CURRENT_TIMESTAMP not null,
    updated_at  timestamp default CURRENT_TIMESTAMP not null,
    primary key (module_id, platform_id)
);


