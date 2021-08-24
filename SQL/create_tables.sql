create table public.location_areas (
    area_id integer primary key,
    area character varying(64)
);
alter table public.location_areas owner to gpuser;

create table public.clients (
    client_id integer primary key,
    location_area_id integer
);
alter table public.clients owner to gpuser;

create table public.stores (
    store_id integer primary key,
    location_area_id smallint
);
alter table public.stores owner to gpuser;

create table public.orders (
    order_id integer,
    product_id integer,
    client_id integer,
    store_id integer,
    quantity integer,
    order_date date
);
alter table public.orders owner to gpuser;

create table public.products (
    product_id integer primary key,
    product_name character varying(255),
    aisle character varying(127),
    department character varying(127)
);
alter table public.products owner to gpuser;

create table public.out_of_stock (
    product_id integer not null,
    order_date date not null
);
alter table public.out_of_stock owner to gpuser;

create table public.dates (
    order_date date primary key,
    order_day integer,
    order_month integer,
    order_year integer
    
);
alter table public.dates owner to gpuser;

alter table only public.clients
    add constraint clients_locationareas_area_id_fk foreign key (location_area_id) references public.location_areas(area_id);
   
alter table only public.stores
	add constraint stores_locationareas_area_id_fk foreign key (location_area_id) references public.location_areas(area_id);

alter table only public.orders
	add constraint orders_clients_client_id_fk foreign key (client_id) references public.clients(client_id);

alter table only public.orders
	add constraint orders_products_product_id_fk foreign key (product_id) references public.products(product_id);

alter table only public.orders
	add constraint orders_stores_store_id_fk foreign key (store_id) references public.stores(store_id);

alter table only public.orders
	add constraint orders_dates_orderdate_fk foreign key (order_date) references public.dates(order_date);

alter table only public.out_of_stock
	add constraint stock_products_product_id_fk foreign key (product_id) references public.products(product_id);

alter table only public.out_of_stock
	add constraint stock_stores_store_id_fk foreign key (store_id) references public.stores(store_id);

alter table only public.out_of_stock
	add constraint stock_dates_orderdate_fk foreign key (order_date) references public.dates(order_date);