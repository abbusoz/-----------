// Coin Trade — real-time crypto prices from Binance public market data (no API key needed).
const COINS=["BTC","ETH","BNB","SOL","XRP","TON","DOGE","TRX","ADA","AVAX"];
const REST="https://data-api.binance.vision/api/v3";
const WS="wss://data-stream.binance.vision/stream?streams=";
const WINDOW=60*60*1000; // chart shows last 60 minutes

const T={
tg:{title:"Хуш омадед",sub:"Барои ворид шудан маълумотро пур кунед",first:"Ном",last:"Фамилия",lang:"Забон",enter:"Ворид шудан",err:"Ном ва фамилияро ворид кунед",hello:"Салом, ",out:"Баромадан",s1:"Коинҳо (озод)",s2:"Сармояи умумӣ",s3:"Фоида / зарар",all:"Бозор",fav:"★ Интихобшуда",buy:"Харидан",sell:"Фурӯхтан",have:"Дар ихтиёр: ",nofav:"Бо ★ ҷуфтро интихоб кунед",bad:"Маблағи дуруст ворид кунед",nomoney:"Коин намерасад",nohold:"Ин қадар дороӣ нест",bought:"Харида шуд: ",sold:"Фурӯхта шуд: ",live:"● зинда",conn:"● пайвастшавӣ...",off:"● қатъ",hi:"Баландтарин 24с",lo:"Пасттарин 24с",vol:"Ҳаҷм 24с",hover:"Нишондодро ба график гузоред",demo:"Нархҳо аз Binance, ҳар сония нав мешаванд. Савдо бо коинҳои виртуалӣ."},
ru:{title:"Добро пожаловать",sub:"Заполните данные для входа",first:"Имя",last:"Фамилия",lang:"Язык",enter:"Войти",err:"Введите имя и фамилию",hello:"Привет, ",out:"Выйти",s1:"Коины (свободно)",s2:"Общий капитал",s3:"Прибыль / убыток",all:"Рынок",fav:"★ Избранное",buy:"Купить",sell:"Продать",have:"В наличии: ",nofav:"Отметьте пару звёздочкой ★",bad:"Введите корректную сумму",nomoney:"Недостаточно коинов",nohold:"Нет столько актива",bought:"Куплено: ",sold:"Продано: ",live:"● live",conn:"● подключение...",off:"● нет связи",hi:"Макс 24ч",lo:"Мин 24ч",vol:"Объём 24ч",hover:"Наведите на график",demo:"Цены с Binance, обновляются каждую секунду. Торговля виртуальными коинами."},
en:{title:"Welcome",sub:"Fill in your details to sign in",first:"First name",last:"Last name",lang:"Language",enter:"Sign in",err:"Enter first and last name",hello:"Hello, ",out:"Sign out",s1:"Coins (free)",s2:"Total equity",s3:"Profit / loss",all:"Market",fav:"★ Favorites",buy:"Buy",sell:"Sell",have:"Holding: ",nofav:"Tap ★ to add pairs",bad:"Enter a valid amount",nomoney:"Not enough coins",nohold:"Not enough of this asset",bought:"Bought: ",sold:"Sold: ",live:"● live",conn:"● connecting...",off:"● offline",hi:"24h high",lo:"24h low",vol:"24h volume",hover:"Hover the chart",demo:"Prices from Binance, updated every second. Trading uses virtual coins."}};

const $=i=>document.getElementById(i);
const KEY="coin_site_users",CUR="coin_site_current";
const load=()=>{try{return JSON.parse(localStorage.getItem(KEY))||{}}catch(e){return{}}};
const persist=()=>{if(!user)return;const u=load();u[user.id]=user;try{localStorage.setItem(KEY,JSON.stringify(u))}catch(e){}};
const getCur=()=>{try{return localStorage.getItem(CUR)}catch(e){return null}};
const setCur=v=>{try{v?localStorage.setItem(CUR,v):localStorage.removeItem(CUR)}catch(e){}};

let lang="tg",user=null,sel="BTC",favOnly=false,status="conn",hover=null,lastTick=0,raf=0;
const D={};   // latest ticker data per coin
const S={};   // price series per coin: [[time,price],...]
COINS.forEach(c=>S[c]=[]);

const fmt=n=>n>=100?n.toLocaleString("en",{maximumFractionDigits:2}):n>=1?n.toFixed(3):n.toFixed(5);
const big=n=>n>=1e9?(n/1e9).toFixed(2)+"B":n>=1e6?(n/1e6).toFixed(2)+"M":Math.round(n).toLocaleString("en");
const price=s=>D[s]?D[s].p:0;

/* ---------- data ---------- */
function onTick(d){
 const s=d.s.replace("USDT","");if(!S[s])return;
 const p=+d.c,now=Date.now();
 D[s]={p,ch:+d.P,hi:+d.h,lo:+d.l,vol:+d.q};
 const a=S[s];if(!a.length||now-a[a.length-1][0]>800)a.push([now,p]);else a[a.length-1][1]=p;
 while(a.length&&now-a[0][0]>WINDOW)a.shift();
 lastTick=now;schedule();
}
async function loadHistory(s){
 try{
  const r=await fetch(`${REST}/klines?symbol=${s}USDT&interval=1m&limit=60`);
  const k=await r.json();if(!Array.isArray(k))return;
  const h=k.map(x=>[x[6],+x[4]]);
  const first=S[s].length?S[s][0][0]:Infinity;
  S[s]=h.filter(x=>x[0]<first).concat(S[s]);
  if(!D[s]&&h.length){D[s]={p:h[h.length-1][1],ch:0,hi:0,lo:0,vol:0}}
  schedule();
 }catch(e){}
}
async function poll(){
 try{
  const sy=encodeURIComponent(JSON.stringify(COINS.map(c=>c+"USDT")));
  const r=await fetch(`${REST}/ticker/24hr?symbols=${sy}`);
  (await r.json()).forEach(t=>onTick({s:t.symbol,c:t.lastPrice,P:t.priceChangePercent,h:t.highPrice,l:t.lowPrice,q:t.quoteVolume}));
 }catch(e){}
}
function connect(){
 setStatus("conn");
 const ws=new WebSocket(WS+COINS.map(c=>c.toLowerCase()+"usdt@ticker").join("/"));
 ws.onopen=()=>setStatus("live");
 ws.onmessage=e=>{try{onTick(JSON.parse(e.data).data)}catch(_){}};
 ws.onclose=()=>{setStatus("off");setTimeout(connect,3000)};
 ws.onerror=()=>ws.close();
}
function setStatus(s){status=s;if(user)$("live").textContent=T[lang][s],$("live").style.color=s==="live"?"var(--up)":"var(--muted)"}
setInterval(()=>{if(Date.now()-lastTick>4000)poll()},3000); // REST fallback if WebSocket is silent

/* ---------- UI ---------- */
function tr(){
 const t=T[lang],m={"t-title":"title","t-sub":"sub","l-first":"first","l-last":"last","l-lang":"lang",enter:"enter",out:"out",s1:"s1",s2:"s2",s3:"s3",tAll:"all",tFav:"fav",buy:"buy",sell:"sell",demo:"demo"};
 for(const id in m)$(id).textContent=t[m[id]];
 if(user)$("hello").textContent=t.hello+user.first+" "+user.last+"!";
 $("lang").value=lang;$("lang2").value=lang;setStatus(status);if(user)render();
}
const equity=()=>{let e=user.coins;for(const s in user.hold)e+=user.hold[s]*price(s);return e};
function schedule(){if(!user||raf)return;raf=requestAnimationFrame(()=>{raf=0;render()})}
function render(){
 const t=T[lang];let h="",n=0;
 COINS.forEach(s=>{const d=D[s],f=user.fav.includes(s);if(favOnly&&!f)return;n++;
  const ch=d?d.ch:0;
  h+=`<div class="row${s===sel?" sel":""}" data-s="${s}"><span class="star${f?" on":""}" data-f="${s}">${f?"★":"☆"}</span><span><b>${s}</b><small>/USDT</small></span><span class="num">${d?fmt(d.p):"…"}</span><span class="num ${ch>=0?"up":"dn"}">${ch>=0?"+":""}${ch.toFixed(2)}%</span></div>`});
 $("list").innerHTML=n?h:`<p>${t.nofav}</p>`;
 const d=D[sel],a=S[sel];
 $("pair").textContent=sel+"/USDT";
 if(d){$("price").textContent=fmt(d.p);
  const up=a.length<2||a[a.length-1][1]>=a[a.length-2][1];$("price").className=up?"up":"dn";
  $("ext").innerHTML=`<span>${t.hi}: ${fmt(d.hi)}</span><span>${t.lo}: ${fmt(d.lo)}</span><span>${t.vol}: ${big(d.vol)} USDT</span>`}
 $("bal").textContent=user.coins.toFixed(2);$("eq").textContent=equity().toFixed(2);
 const pl=equity()-100;$("pnl").textContent=(pl>=0?"+":"")+pl.toFixed(2);$("pnl").className=pl>=0?"up":"dn";
 $("hold").textContent=t.have+(user.hold[sel]||0).toFixed(6)+" "+sel;
 chart();
}
function chart(){
 const a=S[sel],cv=$("ch"),dpr=window.devicePixelRatio||1,w=cv.clientWidth,hh=cv.clientHeight;
 if(cv.width!==Math.round(w*dpr)){cv.width=Math.round(w*dpr);cv.height=Math.round(hh*dpr)}
 const x=cv.getContext("2d");x.setTransform(dpr,0,0,dpr,0,0);x.clearRect(0,0,w,hh);
 if(a.length<2)return;
 const cs=getComputedStyle(document.documentElement),g=n=>cs.getPropertyValue(n).trim();
 const up=a[a.length-1][1]>=a[0][1],col=g(up?"--up":"--dn"),mu=g("--muted"),bd=g("--border");
 let mn=Infinity,mx=-Infinity;a.forEach(p=>{mn=Math.min(mn,p[1]);mx=Math.max(mx,p[1])});
 const pad=(mx-mn)*.12||mx*.001;mn-=pad;mx+=pad;
 const L=6,R=70,B=22,Tp=8,pw=w-L-R,ph=hh-B-Tp,t0=a[0][0],t1=a[a.length-1][0]||t0+1;
 const X=t=>L+(t-t0)/(t1-t0||1)*pw,Y=v=>Tp+(1-(v-mn)/(mx-mn))*ph;
 x.font="11px system-ui";x.lineWidth=1;
 for(let i=0;i<=4;i++){const v=mn+(mx-mn)*i/4,y=Y(v);x.strokeStyle=bd;x.beginPath();x.moveTo(L,y);x.lineTo(L+pw,y);x.stroke();x.fillStyle=mu;x.fillText(fmt(v),L+pw+5,y+4)}
 for(let i=0;i<=4;i++){const t=t0+(t1-t0)*i/4,d=new Date(t);x.fillStyle=mu;x.fillText(d.toLocaleTimeString([],{hour:"2-digit",minute:"2-digit"}),Math.min(X(t),w-R-30),hh-6)}
 x.beginPath();a.forEach((p,i)=>i?x.lineTo(X(p[0]),Y(p[1])):x.moveTo(X(p[0]),Y(p[1])));
 x.strokeStyle=col;x.lineWidth=2;x.lineJoin="round";x.stroke();
 x.lineTo(X(t1),Tp+ph);x.lineTo(L,Tp+ph);x.closePath();
 const gr=x.createLinearGradient(0,0,0,hh);gr.addColorStop(0,col+"44");gr.addColorStop(1,col+"00");x.fillStyle=gr;x.fill();
 const last=a[a.length-1][1],ly=Y(last);
 x.setLineDash([4,4]);x.strokeStyle=col;x.lineWidth=1;x.beginPath();x.moveTo(L,ly);x.lineTo(L+pw,ly);x.stroke();x.setLineDash([]);
 x.fillStyle=col;x.fillRect(L+pw,ly-9,R-2,18);x.fillStyle="#fff";x.fillText(fmt(last),L+pw+5,ly+4);
 x.beginPath();x.arc(X(t1),ly,4,0,7);x.fillStyle=col;x.fill();
 // crosshair
 const tip=$("tip");
 if(hover!==null){
  const tt=t0+(hover-L)/pw*(t1-t0);let b=a[0];a.forEach(p=>{if(Math.abs(p[0]-tt)<Math.abs(b[0]-tt))b=p});
  const cx=X(b[0]),cy=Y(b[1]);
  x.strokeStyle=mu;x.setLineDash([3,3]);x.beginPath();x.moveTo(cx,Tp);x.lineTo(cx,Tp+ph);x.moveTo(L,cy);x.lineTo(L+pw,cy);x.stroke();x.setLineDash([]);
  x.beginPath();x.arc(cx,cy,5,0,7);x.fillStyle=col;x.fill();
  tip.textContent=new Date(b[0]).toLocaleTimeString()+"  —  "+fmt(b[1])+" USDT";
 }else tip.textContent=T[lang].hover;
}
function move(e){const r=$("ch").getBoundingClientRect(),p=e.touches?e.touches[0]:e;hover=p.clientX-r.left;chart()}
function leave(){hover=null;chart()}
const cv=$("ch");
cv.addEventListener("mousemove",move);cv.addEventListener("mouseleave",leave);
cv.addEventListener("touchstart",move,{passive:true});cv.addEventListener("touchmove",move,{passive:true});cv.addEventListener("touchend",leave);

/* ---------- trading ---------- */
function say(s,ok){$("msg").textContent=s;$("msg").style.color=ok?"var(--up)":"var(--dn)"}
function trade(buy){
 const t=T[lang],a=parseFloat($("amt").value),p=price(sel);
 if(!(a>0)||!p)return say(t.bad);
 if(buy){if(a>user.coins+1e-9)return say(t.nomoney);user.coins-=a;user.hold[sel]=(user.hold[sel]||0)+a/p;say(t.bought+(a/p).toFixed(6)+" "+sel,1)}
 else{const q=a/p;if(q>(user.hold[sel]||0)+1e-12)return say(t.nohold);user.hold[sel]-=q;user.coins+=a;say(t.sold+q.toFixed(6)+" "+sel,1)}
 persist();render();
}
$("buy").onclick=()=>trade(1);$("sell").onclick=()=>trade(0);
$("list").onclick=e=>{
 const f=e.target.getAttribute("data-f");
 if(f){const i=user.fav.indexOf(f);i>=0?user.fav.splice(i,1):user.fav.push(f);persist();render();return}
 const r=e.target.closest(".row");
 if(r){sel=r.dataset.s;hover=null;render();if(innerWidth<760)$("detail").scrollIntoView({behavior:"smooth"})}
};
const tab=f=>{favOnly=f;$("tAll").className=f?"g":"";$("tFav").className=f?"":"g";render()};
$("tAll").onclick=()=>tab(false);$("tFav").onclick=()=>tab(true);

/* ---------- login ---------- */
function show(){const u=!!user;$("login").classList.toggle("hidden",u);$("cab").classList.toggle("hidden",!u);tr()}
$("lang").onchange=function(){lang=this.value;tr()};
$("lang2").onchange=function(){lang=this.value;user.lang=lang;persist();tr()};
$("enter").onclick=()=>{
 const f=$("first").value.trim(),l=$("last").value.trim();
 if(!f||!l){$("err").textContent=T[lang].err;return}
 const id=(f+"_"+l).toLowerCase(),u=load(),o=u[id]||{id,first:f,last:l,coins:100};
 o.fav=o.fav||[];o.hold=o.hold||{};o.lang=lang;user=o;persist();setCur(id);$("err").textContent="";show();
};
$("out").onclick=()=>{setCur(null);user=null;$("first").value="";$("last").value="";show()};
addEventListener("resize",()=>{if(user)chart()});
(()=>{const id=getCur(),u=load();
 if(id&&u[id]){user=u[id];user.fav=user.fav||[];user.hold=user.hold||{};lang=user.lang||"tg"}
 show();COINS.forEach(loadHistory);connect();poll();
})();
