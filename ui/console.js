const el = id => document.getElementById(id);
const node = (tag, text, className) => {
  const n = document.createElement(tag);
  if (text !== undefined) n.textContent = text;
  if (className) n.className = className;
  return n;
};
let controller, model, running = false;
async function refresh() {
  try {
    const response = await fetch('/backends', {cache:'no-store', signal:AbortSignal.timeout(3000)});
    if (!response.ok) throw Error();
    const data = await response.json();
    model = data.model;
    for (const k of ['admitted','queued','failovers','model']) el(k).textContent = data[k];
    el('active').textContent = data.active + ' / ' + data.capacity;
    el('connection').textContent = data.accepting ? 'Gateway online' : 'Draining';
    el('connection').classList.toggle('ok', data.accepting);
    const total = data.backends.reduce((n,w) => n+w.accepted_headers, 0);
    el('workers').replaceChildren(...data.backends.map(w => {
      const block = node('article', undefined, 'worker');
      const title = node('div');
      title.append(node('strong', 'Worker '+w.name), node('span',w.circuit,'pill'));
      const track = node('div', undefined,'track');
      const bar = node('i');
      bar.style.width = (total ? 100*w.accepted_headers/total : 0)+'%';
      track.append(bar);
      block.append(title,track,node('span',w.accepted_headers+' accepted · '+w.failures+' consecutive failures','model'));
      return block;
    }));
  } catch {
    el('connection').textContent = 'Telemetry offline';
    el('connection').classList.remove('ok');
  }
}
async function poll() { await refresh(); setTimeout(poll,2000); }
poll();
el('cancel').onclick = () => controller?.abort();

// Complete SSE events can span reads. Decode UTF-8 incrementally and require
// the completion sentinel so an interrupted stream cannot look successful.
async function readStream(response, onContent) {
  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
  let buffer = '';
  try {
    while (true) {
      const {value,done} = await reader.read();
      if (done) throw Error('Stream ended before [DONE]');
      buffer += value;
      let boundary;
      while ((boundary = /\r?\n\r?\n/.exec(buffer))) {
        const frame = buffer.slice(0,boundary.index);
        buffer = buffer.slice(boundary.index+boundary[0].length);
        const data = frame.split(/\r?\n/).filter(l=>l.startsWith('data:'))
          .map(l=>l.slice(5).replace(/^ /,'')).join('\n');
        if (!data) continue;
        if (data === '[DONE]') return;
        const content = JSON.parse(data).choices?.[0]?.delta?.content;
        if (content) onContent(content);
      }
      if (buffer.length > 1_048_576) throw Error('SSE event too large');
    }
  } finally { await reader.cancel().catch(()=>{}); reader.releaseLock(); }
}
el('request').onsubmit = async event => {
  event.preventDefault();
  if (running) return;
  running = true;
  controller = new AbortController();
  el('run').disabled = true; el('cancel').disabled = false;
  const start = performance.now();
  let first = '—', id = '—', worker = '—', status = 'Sending', output = '';
  el('result').textContent = status; el('output').textContent = '';
  const ms = () => Math.round(performance.now()-start)+' ms';
  try {
    const stream = el('mode').value === 'stream';
    const response = await fetch('/v1/chat/completions',{method:'POST',signal:controller.signal,
      headers:{'Content-Type':'application/json',Authorization:'Bearer '+el('key').value},
      body:JSON.stringify({model,stream,max_tokens:96,messages:[{role:'user',content:el('prompt').value}]})});
    id = response.headers.get('x-request-id') || '—';
    worker = response.headers.get('x-backend') || '—';
    if (!response.ok) throw Error(response.status+' '+(await response.text()).slice(0,250));
    if (stream) {
      el('result').textContent = 'Streaming';
      await readStream(response, text => {
        if (first === '—') first = ms();
        output = (output+text).slice(-16000);
        el('output').textContent = output;
      });
    } else { el('output').textContent = JSON.stringify(await response.json(),null,2).slice(0,16000); }
    status = 'Complete';
  } catch (e) {
    status = controller.signal.aborted ? 'Cancelled' : 'Failed';
    el('output').textContent = [output,status === 'Cancelled' ? 'Request cancelled.' : e.message].filter(Boolean).join('\n');
  } finally {
    const row = node('tr');
    row.append(...[id,worker,status,first,ms()].map(value=>node('td',value)));
    el('history').prepend(row);
    if (el('history').children.length>10) el('history').lastElementChild.remove();
    el('empty').hidden = true; el('result').textContent = status;
    el('run').disabled = false; el('cancel').disabled = true; running = false;
    refresh();
  }
};
