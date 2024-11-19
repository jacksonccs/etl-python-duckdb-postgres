--drop table vendas
create table vendas(
	venda_id serial primary key,
	data_venda date not null,
	valor decimal(10,2) not null,
	quantidade int not null,
	cliente_id int not null,
	categoria varchar(255) not null
)

--drop table vendas_calculado
create table vendas_calculado(
	venda_id serial primary key,
	data_venda date not null,
	valor decimal(10,2) not null,
	quantidade int not null,
	cliente_id int not null,
	categoria varchar(255) not null,
	total_vendas decimal(10,2) not null
)

select * from vendas
select * from vendas_calculado

insert into vendas(data_venda,valor,quantidade,cliente_id,categoria) values 
('2024-01-05',450.25,2,14,'Brinquedos'),
('2024-01-05',500.0,4,22,'Livros'),
('2024-01-05',95.5,5,19,'Alimentos'),
('2024-01-05',380.75,3,3,'Eletrônicos'),
('2024-01-05',460.0,7,11,'Livros'),
('2024-01-05',140.25,9,17,'Roupas'),
('2024-01-05',360.6,6,1,'Alimentos'),
('2024-01-05',290.4,1,28,'Brinquedos'),
('2024-01-05',240.5,8,20,'Eletrônicos'),
('2024-01-05',155.0,3,45,'Roupas')

--

insert into vendas_calculado(data_venda,valor,quantidade,cliente_id,categoria,total_vendas) 
select  data_venda,valor,quantidade,cliente_id,categoria,(valor * quantidade) as total_vendas from vendas

