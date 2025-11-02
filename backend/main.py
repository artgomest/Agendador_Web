# ==============================================================
# ROTA ÁGIL - BACKEND (Cloud Run)
# Versão otimizada para inicialização rápida
# ==============================================================

import os
import json
import re
import time
from datetime import datetime, date, timedelta, timezone
from typing import Dict, Any, List, Optional

from flask import Flask, request

# Importações externas (só os módulos, sem inicializar nada ainda)
import google.auth
import googlemaps
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import firebase_admin
from firebase_admin import credentials, firestore

# ==============================================================
# CONSTANTES GERAIS
# ==============================================================
SCOPES = ['https://www.googleapis.com/auth/calendar.events']
FUSO_HORARIO_LOCAL = timezone(timedelta(hours=-3))
UTC_OFFSET = timedelta(hours=3)

# ==============================================================
# INICIALIZAÇÃO FLASK
# ==============================================================
app = Flask(__name__)

# ==============================================================
# FUNÇÃO AUXILIAR – Inicializa Firebase apenas quando necessário
# ==============================================================
def get_firestore_client():
    """Inicializa o Firebase e retorna um cliente do Firestore (lazy loading)."""
    if not firebase_admin._apps:
        firebase_admin.initialize_app()

        db = firestore.client()
    return firebase_admin.firestore.client()

# ==============================================================
# FUNÇÕES AUXILIARES (inalteradas)
# ==============================================================
def parse_distancia_para_valor(distancia_texto: Optional[str]) -> float:
    if not distancia_texto or distancia_texto.upper() == "N/A": return 0.0
    texto_limpo = str(distancia_texto).lower().replace(',', '.')
    try:
        if "km" in texto_limpo: return float(re.sub(r"[^\d.]", "", texto_limpo))
        elif "m" in texto_limpo: return float(re.sub(r"[^\d.]", "", texto_limpo)) / 1000.0
    except (ValueError, TypeError):
        print(f"Alerta: Não foi possível converter distância '{distancia_texto}'")
    return float('inf')

def classificar_evento(event_summary: str, event_location: str) -> str:
    summary_upper = event_summary.upper()
    if "PROJETO ENANI" in summary_upper: return "INDISPONIVEL_MANHA (PROJETO ENANI)"
    if "ORIGEM" in summary_upper or "CASA" in summary_upper: return "PONTO_ORIGEM"
    if any(keyword in summary_upper for keyword in ["FOLGA", "RESERVADO", "INDISPONIVEL", "DENTISTA", "PESSOAL", "BLOQUEADO", "NÃO MARCAR"]): 
        return "BLOQUEIO_GERAL"
    is_os = "OS" in event_summary
    tem_telefone = bool(re.search(r'\b(\(?\d{2}\)?\s?)?(9?\d{4,5}-?\d{4})\b', event_summary))
    if is_os: return "COLETA_CONFIRMADA (OS)"
    if "ROTA NEFRO" in summary_upper: return "COLETA_ESPECIAL (ROTA NEFRO)"
    if event_location and event_location != 'Local não especificado':
        if tem_telefone: return "COLETA_A_CONFIRMAR (Telefone)"
        if not any(bloqueio in summary_upper for bloqueio in ["PROJETO ENANI", "ORIGEM", "CASA"]): 
            return "COLETA_A_CONFIRMAR (Nome Paciente?)"
    return "EVENTO_DESCONHECIDO"

def extrair_endereco_otimizado(descricao: Optional[str], local: Optional[str]) -> str:
    if descricao:
        for linha in descricao.splitlines():
            match = re.search(r"^\s*(?:📍)?\s*[Ee]ndere[çcÇ]o:\s*(.*)", linha.strip(), re.IGNORECASE)
            if match:
                return match.group(1).strip()
    return local if local else "Local não especificado"

# --- FUNÇÕES DE LÓGICA DE NEGÓCIO ---

def _preparar_dados_agendas(gmaps_client: googlemaps.Client, calendar_service: Any, dias_de_busca: int, calendarios_colhedores: Dict[str, str]) -> List[Dict[str, Any]]:
    print(f"Iniciando preparação de dados para {len(calendarios_colhedores)} colhedores.")
    cronogramas_processados = []
    if not calendarios_colhedores:
        print("INFO: Nenhum calendário de colhedor configurado.")
        return []
    
    today_local_date = datetime.now(FUSO_HORARIO_LOCAL).date()
    
    for nome_colhedor, calendar_id in calendarios_colhedores.items():
        for i in range(1, dias_de_busca + 1):
            current_local_date = today_local_date + timedelta(days=i)
            dia_info = {
                'colhedor_nome': nome_colhedor, 'data_iso': current_local_date.isoformat(),
                'ponto_partida_sumario': None, 'ponto_partida_endereco_str': None,
                'ponto_partida_coords': None, 'horario_partida_dt_local': None,
                'dia_totalmente_indisponivel': False, 'motivo_indisponibilidade': None,
                'compromissos': []
            }

            time_min_utc_str = (datetime.combine(current_local_date, datetime.min.time()) + UTC_OFFSET).isoformat() + 'Z'
            time_max_utc_str = (datetime.combine(current_local_date, datetime.max.time()) + UTC_OFFSET).isoformat() + 'Z'

            try:
                events_result = calendar_service.events().list(
                    calendarId=calendar_id, timeMin=time_min_utc_str, timeMax=time_max_utc_str,
                    maxResults=50, singleEvents=True, orderBy='startTime').execute()
                eventos_do_dia = events_result.get('items', [])

                for event in eventos_do_dia:
                    sumario = event.get('summary', 'Evento sem título')
                    local_evento = extrair_endereco_otimizado(event.get('description'), event.get('location'))
                    tipo = classificar_evento(sumario, local_evento)
                    
                    inicio_str = event['start'].get('dateTime', event['start'].get('date'))
                    fim_str = event['end'].get('dateTime', event['end'].get('date'))
                    
                    compromisso = {
                        'sumario': sumario, 'tipo': tipo,
                        'inicio_dt_local': datetime.fromisoformat(inicio_str.replace('Z', '+00:00')).astimezone(FUSO_HORARIO_LOCAL).isoformat() if 'T' in inicio_str else None,
                        'fim_dt_local': datetime.fromisoformat(fim_str.replace('Z', '+00:00')).astimezone(FUSO_HORARIO_LOCAL).isoformat() if 'T' in fim_str else None,
                        'local_endereco_str': local_evento,
                        'local_coords': None
                    }
                    
                    if tipo == "PONTO_ORIGEM" and compromisso['inicio_dt_local'] and not dia_info['ponto_partida_sumario']:
                        dia_info.update({
                            'ponto_partida_sumario': sumario,
                            'ponto_partida_endereco_str': compromisso['local_endereco_str'],
                            'horario_partida_dt_local': compromisso['inicio_dt_local']
                        })
                        try:
                            geocode_result = gmaps_client.geocode(compromisso['local_endereco_str'])
                            if geocode_result:
                                dia_info['ponto_partida_coords'] = geocode_result[0]['geometry']['location']
                        except Exception as e:
                            print(f"Erro ao geocodificar ponto de partida: {e}")

                    dia_info['compromissos'].append(compromisso)
            except HttpError as error:
                print(f"Erro ao buscar eventos para {nome_colhedor}: {error}")
            
            cronogramas_processados.append(dia_info)
    
    print(f"Preparação de dados concluída. {len(cronogramas_processados)} dias/colhedores processados.")
    return cronogramas_processados

def _encontrar_slots_para_paciente(cronogramas_processados: List[Dict[str, Any]], endereco_paciente: str, duracao_minutos: int, gmaps_client: googlemaps.Client, pesos_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    print(f"\n--- Buscando slots para Paciente em: {endereco_paciente} ---")
    slots_viaveis_finais = []
    
    try:
        geocode_paciente_result = gmaps_client.geocode(endereco_paciente)
        if not geocode_paciente_result:
            raise ValueError(f"Endereço do paciente não encontrado: {endereco_paciente}")
        paciente_novo_coords = geocode_paciente_result[0]['geometry']['location']
    except Exception as e_geo_pac:
        raise ValueError(f"Falha ao geocodificar endereço do paciente: {e_geo_pac}")

    # Geocodificar o endereço do laboratório uma vez
    lab_coords = None
    endereco_lab = pesos_config.get('ENDERECO_LABORATORIO')
    if endereco_lab:
        try:
            geocode_lab_result = gmaps_client.geocode(endereco_lab)
            if geocode_lab_result:
                lab_loc = geocode_lab_result[0]['geometry']['location']
                lab_coords = {'lat': lab_loc['lat'], 'lng': lab_loc['lng']}
                print(f"      Coords do Laboratório: {lab_coords}")
        except Exception as e_geo_lab: print(f"ERRO geocodificação laboratório: {e_geo_lab}")
        
    slots_potenciais_brutos = []
    duracao_coleta_td = timedelta(minutes=duracao_minutos) # Define a duração como timedelta

    for dia_info in cronogramas_processados:
        colhedor = dia_info['colhedor_nome'] 
        if dia_info['dia_totalmente_indisponivel']: continue
        horario_partida_val = dia_info.get('horario_partida_dt_local')
        horario_partida_colhedor_dt_local = datetime.fromisoformat(horario_partida_val) if isinstance(horario_partida_val, str) else horario_partida_val
        if not horario_partida_colhedor_dt_local or not dia_info['ponto_partida_coords']: continue
        data_dt = date.fromisoformat(dia_info['data_iso'])
        janela_coleta_inicio_dt_local = datetime.combine(data_dt, time(6,0,0), tzinfo=FUSO_HORARIO_LOCAL)
        if colhedor == "NATIELI":
            janela_coleta_fim_dt_local = datetime.combine(data_dt, time(6, 30, 0), tzinfo=FUSO_HORARIO_LOCAL)
        else:
            janela_coleta_fim_dt_local = datetime.combine(data_dt, time(11,0,0), tzinfo=FUSO_HORARIO_LOCAL)
        compromissos_do_dia_objs = []
        for comp_dict_original in dia_info.get('compromissos', []):
            comp_obj_processed = comp_dict_original.copy()
            start_val_str = comp_obj_processed.get('inicio_dt_local'); end_val_str = comp_obj_processed.get('fim_dt_local')
            comp_obj_processed['inicio_dt_local_obj'] = datetime.fromisoformat(start_val_str) if start_val_str and isinstance(start_val_str, str) else start_val_str
            comp_obj_processed['fim_dt_local_obj'] = datetime.fromisoformat(end_val_str) if end_val_str and isinstance(end_val_str, str) else end_val_str
            if isinstance(comp_obj_processed.get('inicio_dt_local_obj'), datetime) and isinstance(comp_obj_processed.get('fim_dt_local_obj'), datetime):
                compromissos_do_dia_objs.append(comp_obj_processed)
        compromissos_ordenados = sorted(compromissos_do_dia_objs, key=lambda x: x['inicio_dt_local_obj'])
        current_slot_start_dt_local = max(horario_partida_colhedor_dt_local, janela_coleta_inicio_dt_local)
        current_anterior_coords = dia_info['ponto_partida_coords']
        current_anterior_sumario = dia_info.get('ponto_partida_sumario', 'Origem')
        if not compromissos_ordenados:
            if current_slot_start_dt_local < janela_coleta_fim_dt_local:
                slots_potenciais_brutos.append({'colhedor': colhedor, 'data_iso': dia_info['data_iso'],
                    'slot_inicio_dt_local': current_slot_start_dt_local, 'slot_fim_dt_local': janela_coleta_fim_dt_local,
                    'local_anterior_coords': current_anterior_coords, 'sumario_anterior': current_anterior_sumario,
                    'local_posterior_coords': None, 'sumario_posterior': "Fim da Janela"})
        else:
            primeiro_compromisso = compromissos_ordenados[0]
            if current_slot_start_dt_local < primeiro_compromisso['inicio_dt_local_obj']:
                slots_potenciais_brutos.append({'colhedor': colhedor, 'data_iso': dia_info['data_iso'],
                    'slot_inicio_dt_local': current_slot_start_dt_local, 'slot_fim_dt_local': primeiro_compromisso['inicio_dt_local_obj'],
                    'local_anterior_coords': current_anterior_coords, 'sumario_anterior': current_anterior_sumario,
                    'local_posterior_coords': primeiro_compromisso.get('local_coords'), 'sumario_posterior': primeiro_compromisso.get('sumario')})
            current_slot_start_dt_local = max(current_slot_start_dt_local, primeiro_compromisso['fim_dt_local_obj'])
            current_anterior_coords = primeiro_compromisso.get('local_coords', current_anterior_coords)
            current_anterior_sumario = primeiro_compromisso.get('sumario')
            for i in range(len(compromissos_ordenados) - 1):
                comp_seguinte_loop = compromissos_ordenados[i+1]
                if current_slot_start_dt_local < comp_seguinte_loop['inicio_dt_local_obj']:
                    slots_potenciais_brutos.append({'colhedor': colhedor, 'data_iso': dia_info['data_iso'],
                        'slot_inicio_dt_local': current_slot_start_dt_local, 'slot_fim_dt_local': comp_seguinte_loop['inicio_dt_local_obj'],
                        'local_anterior_coords': current_anterior_coords, 'sumario_anterior': current_anterior_sumario,
                        'local_posterior_coords': comp_seguinte_loop.get('local_coords'), 'sumario_posterior': comp_seguinte_loop.get('sumario')})
                current_slot_start_dt_local = max(current_slot_start_dt_local, comp_seguinte_loop['fim_dt_local_obj'])
                current_anterior_coords = comp_seguinte_loop.get('local_coords', current_anterior_coords)
                current_anterior_sumario = comp_seguinte_loop.get('sumario')
            if current_slot_start_dt_local < janela_coleta_fim_dt_local:
                slots_potenciais_brutos.append({'colhedor': colhedor, 'data_iso': dia_info['data_iso'],
                    'slot_inicio_dt_local': current_slot_start_dt_local, 'slot_fim_dt_local': janela_coleta_fim_dt_local,
                    'local_anterior_coords': current_anterior_coords, 'sumario_anterior': current_anterior_sumario,
                    'local_posterior_coords': None, 'sumario_posterior': "Fim da Janela de Coleta"})

    for slot_bruto in slots_potenciais_brutos:
        coords_ant = slot_bruto['local_anterior_coords']
        coords_post = slot_bruto['local_posterior_coords'] # Pode ser None
        slot_inicio_dt = slot_bruto['slot_inicio_dt_local']
        slot_fim_dt = slot_bruto['slot_fim_dt_local']
        
        data_slot_dt = date.fromisoformat(slot_bruto['data_iso'])
        janela_coleta_inicio_dia_dt_local = datetime.combine(data_slot_dt, time(6,0,0), tzinfo=FUSO_HORARIO_LOCAL)
        
        janela_coleta_fim_dia_dt_local = datetime.combine(data_slot_dt, time(11,0,0), tzinfo=FUSO_HORARIO_LOCAL)
        if slot_bruto['colhedor'] == "NATIELI":
            janela_coleta_fim_dia_dt_local = datetime.combine(data_slot_dt, time(6,30,0), tzinfo=FUSO_HORARIO_LOCAL)
            
        if not coords_ant or not paciente_novo_coords: continue

        # Calcular Viagem 1 (Anterior -> Paciente)
        tempo_viagem_ate_paciente_seg, dist_ate_paciente_txt = 0, "N/A"
        try:
            trajeto1_result = gmaps_client.distance_matrix(origins=[coords_ant], destinations=[paciente_novo_coords], mode="driving", language="pt-BR")
            time.sleep(0.02)
            if trajeto1_result['status'] == 'OK' and trajeto1_result['rows'][0]['elements'][0]['status'] == 'OK':
                tempo_viagem_ate_paciente_seg = trajeto1_result['rows'][0]['elements'][0]['duration']['value']
                dist_ate_paciente_txt = trajeto1_result['rows'][0]['elements'][0]['distance']['text']
            else: continue
        except Exception: continue
        
        horario_chegada_paciente_estimado_dt = slot_inicio_dt + timedelta(seconds=tempo_viagem_ate_paciente_seg)
        horario_inicio_coleta_real_dt = max(horario_chegada_paciente_estimado_dt, janela_coleta_inicio_dia_dt_local)
        if horario_inicio_coleta_real_dt < slot_inicio_dt : horario_inicio_coleta_real_dt = max(slot_inicio_dt, janela_coleta_inicio_dia_dt_local)
        horario_fim_coleta_real_dt = horario_inicio_coleta_real_dt + duracao_coleta_td
        
        if not (horario_fim_coleta_real_dt <= janela_coleta_fim_dia_dt_local and horario_fim_coleta_real_dt <= slot_fim_dt): continue

        # Calcular Viagem 2 (Paciente -> Próximo ou Laboratório)
        tempo_viagem_paciente_proximo_seg, dist_paciente_proximo_txt = 0, "N/A"
        sumario_posterior = slot_bruto['sumario_posterior']
        
        # *** NOVA LÓGICA PARA DESTINO POSTERIOR ***
        destino_final_coords = coords_post # Tenta usar as coords do próximo compromisso
        if not destino_final_coords:
            destino_final_coords = lab_coords # Se não houver, usa as coords do laboratório
            sumario_posterior = "Retorno ao Laboratório"
        
        chegada_proximo_destino_dt = horario_fim_coleta_real_dt
        if destino_final_coords: # Se temos um destino (próximo compromisso OU laboratório)
            try:
                trajeto2_result = gmaps_client.distance_matrix(origins=[paciente_novo_coords], destinations=[destino_final_coords], mode="driving", language="pt-BR")
                time.sleep(0.02)
                if trajeto2_result['status'] == 'OK' and trajeto2_result['rows'][0]['elements'][0]['status'] == 'OK':
                    tempo_viagem_paciente_proximo_seg = trajeto2_result['rows'][0]['elements'][0]['duration']['value']
                    dist_paciente_proximo_txt = trajeto2_result['rows'][0]['elements'][0]['distance']['text']
                    chegada_proximo_destino_dt = horario_fim_coleta_real_dt + timedelta(seconds=tempo_viagem_paciente_proximo_seg)
                else: continue
            except Exception: continue
        
        if chegada_proximo_destino_dt <= slot_fim_dt:
            dist_anterior_km_val = parse_distancia_para_valor(dist_ate_paciente_txt)
            dist_posterior_km_val = parse_distancia_para_valor(dist_paciente_proximo_txt)
            slots_viaveis_finais.append({
                'colhedor': slot_bruto['colhedor'], 'data_iso': slot_bruto['data_iso'],
                'horario_proposto_chegada_paciente_dt_local': horario_inicio_coleta_real_dt.isoformat(),
                'horario_proposto_saida_paciente_dt_local': horario_fim_coleta_real_dt.isoformat(),
                'duracao_coleta_min': duracao_minutos,
                'sumario_anterior': slot_bruto['sumario_anterior'],
                'distancia_de_anterior_km_texto': dist_ate_paciente_txt,
                'distancia_anterior_valor_km': dist_anterior_km_val,
                'tempo_viagem_de_anterior_total_seconds': tempo_viagem_ate_paciente_seg,
                'tempo_viagem_de_anterior_min': str(timedelta(seconds=tempo_viagem_ate_paciente_seg)),
                'sumario_posterior': sumario_posterior, # Usa o sumário atualizado
                'distancia_para_posterior_km_texto': dist_paciente_proximo_txt,
                'distancia_posterior_valor_km': dist_posterior_km_val,
                'tempo_viagem_para_posterior_total_seconds': tempo_viagem_paciente_proximo_seg,
                'tempo_viagem_para_posterior_min': str(timedelta(seconds=tempo_viagem_paciente_proximo_seg)),
                'coords_paciente': paciente_novo_coords,
                'coords_anterior': coords_ant, 
                'coords_posterior': destino_final_coords, # Usa o destino final
                'endereco_paciente_str': endereco_paciente
            })

    if not slots_viaveis_finais:
        print("   Nenhum slot VIÁVEL encontrado."); return []
    
    # --- LÓGICA DE PONTUAÇÃO E ORDENAÇÃO (PERMANECE A MESMA) ---
    print(f"INFO: Usando pesos para ordenação: Data={pesos_config['PESO_DATA']}, Tempo={pesos_config['PESO_TEMPO_VIAGEM']}, Dist={pesos_config['PESO_DISTANCIA']}")
    today_date_local = datetime.now(FUSO_HORARIO_LOCAL).date() # Define a data atual
    for slot in slots_viaveis_finais:
        slot_date = date.fromisoformat(slot['data_iso'])
        slot['dias_a_partir_hoje'] = (slot_date - today_date_local).days
        slot['tempo_total_viagem_seg'] = slot['tempo_viagem_de_anterior_total_seconds'] + slot['tempo_viagem_para_posterior_total_seconds']
        slot['distancia_total_km'] = slot.get('distancia_anterior_valor_km', 0.0) + slot.get('distancia_posterior_valor_km', 0.0)
    min_dias = min((s['dias_a_partir_hoje'] for s in slots_viaveis_finais), default=0)
    max_dias = max((s['dias_a_partir_hoje'] for s in slots_viaveis_finais), default=0)
    min_tempo = min((s['tempo_total_viagem_seg'] for s in slots_viaveis_finais), default=0)
    max_tempo = max((s['tempo_total_viagem_seg'] for s in slots_viaveis_finais), default=0)
    distancias_validas = [s['distancia_total_km'] for s in slots_viaveis_finais if s['distancia_total_km'] != float('inf')]
    min_dist = min(distancias_validas, default=0.0) if distancias_validas else 0.0 
    max_dist = max(distancias_validas, default=0.0) if distancias_validas else 0.0 
    for slot in slots_viaveis_finais:
        slot['norm_data'] = (slot['dias_a_partir_hoje'] - min_dias) / (max_dias - min_dias) if max_dias > min_dias else 0
        slot['norm_tempo'] = (slot['tempo_total_viagem_seg'] - min_tempo) / (max_tempo - min_tempo) if max_tempo > min_tempo else 0
        if slot['distancia_total_km'] == float('inf'): slot['norm_dist'] = 1.0 
        elif max_dist > min_dist: slot['norm_dist'] = (slot['distancia_total_km'] - min_dist) / (max_dist - min_dist)
        else: slot['norm_dist'] = 0
        slot['score_final'] =   (pesos_config['data'] * slot['norm_data']) + \
                                (pesos_config['tempoViagem'] * slot['norm_tempo']) + \
                                (pesos_config['distancia'] * slot['norm_dist'])
    slots_viaveis_finais.sort(key=lambda x: x['score_final'])
            
    print(f"\n\n--- SLOTS VIÁVEIS FINAIS (ordenados por SCORE) ---")
    if slots_viaveis_finais:
        def final_converter(o):
            if isinstance(o, (datetime, date, timedelta)): return o.isoformat()
        print(json.dumps(slots_viaveis_finais, indent=2, default=final_converter, ensure_ascii=False))
    else: print("   Nenhum slot VIÁVEL encontrado.")
    
    return slots_viaveis_finais

def _encontrar_e_salvar_slots(empresa_id: str, config: Dict[str, Any]) -> int:
    print(f"Iniciando cálculo COMPLETO de slots para a empresa: {empresa_id}")

    # Inicializações apenas quando necessárias
    gmaps_client = googlemaps.Client(key=os.getenv('GOOGLE_API_KEY'))
    creds, _ = google.auth.default(scopes=SCOPES)
    calendar_service = build('calendar', 'v3', credentials=creds)
    db = get_firestore_client()

    cronogramas = _preparar_dados_agendas(
        gmaps_client, calendar_service,
        config.get('diasDeBusca', 7),
        config.get('calendariosColhedores', {})
    )

    slots_pre_calculados = _encontrar_slots_para_paciente(
        cronogramas,
        config.get('enderecoLaboratorio', ''),
        30, # Duração padrão de 30 minutos
        gmaps_client,
        config.get('pesos', {})
    )

    slots_ref = db.collection('empresas').document(empresa_id).collection('slots_pre_calculados')
    
    # Limpa os slots antigos
    for doc in slots_ref.stream():
        doc.reference.delete()

    # Adiciona os novos slots
    for slot in slots_pre_calculados:
        # Cria um ID único para o slot
        slot_id = f"{slot.get('colhedor', 'N/A')}-{slot.get('horario_proposto_chegada_paciente_dt_local', 'N/A')}"
        slot['id'] = slot_id
        slots_ref.document(slot_id).set(slot)
        
    print(f"Firestore atualizado. {len(slots_pre_calculados)} slots foram salvos para {empresa_id}.")
    return len(slots_pre_calculados)


# --- ROTAS DA API ---

@app.route("/", methods=["POST"])
def executar_calculo_agendado():
    print("Tarefa agendada (cálculo noturno) recebida.")
    try:
        db = get_firestore_client()
        empresas = db.collection('empresas').stream()
        for empresa in empresas:
            _encontrar_e_salvar_slots(empresa.id, empresa.to_dict())
        return "Processo de cálculo noturno concluído.", 200
    except Exception as e:
        print(f"ERRO CRÍTICO no cálculo agendado: {e}")
        return f"Erro interno do servidor: {e}", 500

@app.route("/atualizar-cache-incremental", methods=["POST"])
def executar_atualizacao_incremental():
    print("Tarefa agendada (atualização incremental) recebida.")
    return "Atualização incremental executada.", 200

# ==============================================================
# MAIN LOCAL
# ==============================================================
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))