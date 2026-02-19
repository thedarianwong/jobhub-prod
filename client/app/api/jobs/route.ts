import { NextResponse } from 'next/server';
import { Pool } from 'pg';

const pool = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT),
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
});

export async function GET() {
  const client = await pool.connect();
  try {
    const result = await client.query(
      'SELECT id, title, city, location, company, job_type, date_posted, job_url FROM jobs ORDER BY date_posted DESC'
    );
    return NextResponse.json(result.rows);
  } catch (error) {
    return NextResponse.json({ error: 'Failed to fetch jobs' }, { status: 500 });
  } finally {
    client.release();
  }
}
