import express from 'express';
import bodyParser from 'body-parser';
import cassandra from 'cassandra-driver';
import path from 'path';

const CASSANDRA_HOST = process.env.CASSANDRA_HOST || '127.0.0.1'; // '127.0.0.1' como fallback
const CASSANDRA_DATACENTER = process.env.CASSANDRA_DATACENTER || 'datacenter1';
const KEYSPACE = 'movie_reviews';

const Uuid = cassandra.types.Uuid;

const client = new cassandra.Client({
    contactPoints: [CASSANDRA_HOST],
    localDataCenter: CASSANDRA_DATACENTER,
    // policies: {
    //     retry: new cassandra.policies.retry.RetryPolicy()
    // },
    // queryOptions: { consistency: cassandra.types.consistencies.localQuorum }
});

// --- "Migration" Programática: Criar Keyspace e Tabelas ---

async function initDb() {
    try {
        console.log("Conectando ao Cassandra...");
        await client.connect();
        console.log(`Conectado a ${client.hosts.length} hosts do cluster.`);

        console.log("Criando Keyspace (se não existir)...");
        await client.execute(`
            CREATE KEYSPACE IF NOT EXISTS ${KEYSPACE}
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        `);

        // Informa ao client para usar este keyspace em todas as queries futuras
        client.keyspace = KEYSPACE;

        console.log("Criando tabela 'filmes' (se não existir)...");
        await client.execute(`
            CREATE TABLE IF NOT EXISTS filmes (
                id uuid PRIMARY KEY,
                nome text
            )
        `);

        console.log("Criando tabela 'avaliacoes_por_filme' (se não existir)...");
        await client.execute(`
            CREATE TABLE IF NOT EXISTS avaliacoes_por_filme (
                id_filme uuid,
                data timestamp,
                id_avaliacao uuid,
                usuario text,
                nota int,
                PRIMARY KEY (id_filme, data, id_avaliacao)
            ) WITH CLUSTERING ORDER BY (data DESC)
        `);

        console.log("Banco de dados pronto!");

    } catch (err) {
        console.error("ERRO AO INICIALIZAR O BANCO:", err);
        process.exit(1); // Encerra a aplicação se não conseguir conectar/criar tabelas
    }
}

// --- Endpoints da API ---
const app = express();

app.use(bodyParser.json());

app.use(express.static(path.join(import.meta.dirname, '/public')));

// 1. Carregar a lista de filmes
app.get('/api/filmes', async (req, res) => {
    try {
        const query = 'SELECT id, nome FROM filmes';
        const result = await client.execute(query);
        res.json(result.rows);
    } catch (err) {
        console.error("Erro ao buscar filmes:", err);
        res.status(500).json({ error: 'Erro ao buscar filmes' });
    }
});

// 2. Cadastrar um novo filme
app.post('/api/filmes', async (req, res) => {
    try {
        const { nome } = req.body;
        if (!nome) {
            return res.status(400).json({ error: 'O campo "nome" é obrigatório' });
        }
        
        const id = Uuid.random(); // Gera um UUID v4
        const query = 'INSERT INTO filmes (id, nome) VALUES (?, ?)';
        
        await client.execute(query, [id, nome], { prepare: true });
        
        // Retorna o filme criado com o ID gerado
        res.status(201).json({ id, nome });

    } catch (err) {
        console.error("Erro ao adicionar filme:", err);
        res.status(500).json({ error: 'Erro ao adicionar filme' });
    }
});

// 3. Exibir a lista de avaliações de um filme
app.get('/api/filmes/:id_filme/avaliacoes', async (req, res) => {
    try {
        const { id_filme } = req.params;
        const query = 'SELECT id_filme, usuario, nota, data FROM avaliacoes_por_filme WHERE id_filme = ?';
        
        const result = await client.execute(query, [id_filme], { prepare: true });
        
        res.json(result.rows);

    } catch (err) {
        console.error("Erro ao buscar avaliações:", err);
        res.status(500).json({ error: 'Erro ao buscar avaliações' });
    }
});

// 4. Avaliar (armazenar avaliação) um filme
app.post('/api/filmes/:id_filme/avaliacoes', async (req, res) => {
    try {
        const { id_filme } = req.params;
        const { usuario, nota } = req.body;

        if (!usuario || nota === undefined) {
            return res.status(400).json({ error: 'Campos "usuario" e "nota" são obrigatórios' });
        }

        const data = new Date(); // Data atual
        const id_avaliacao = Uuid.random(); // ID único para a avaliação
        const notaInt = parseInt(nota, 10);

        const query = 'INSERT INTO avaliacoes_por_filme (id_filme, data, id_avaliacao, usuario, nota) VALUES (?, ?, ?, ?, ?)';
        
        await client.execute(query, [id_filme, data, id_avaliacao, usuario, notaInt], { prepare: true });

        res.status(201).json({ 
            id_filme, 
            data, 
            id_avaliacao, 
            usuario, 
            nota: notaInt 
        });

    } catch (err) {
        console.error("Erro ao enviar avaliação:", err);
        res.status(500).json({ error: 'Erro ao enviar avaliação' });
    }
});

const port = process.env.PORT || 3000;

// --- Iniciar o Servidor ---
// Primeiro inicializa o DB, depois inicia o servidor Express
initDb().then(() => {
    app.listen(port, () => {
        console.log(`Servidor rodando em http://localhost:${port}`);
    });
}).catch(err => {
    console.error("Falha ao iniciar o servidor:", err);
});