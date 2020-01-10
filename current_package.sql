
/*
	Subsystem Name: Motion
	Package Name: p_jlp_moves
*/

-- Package Specification

-- procedures
procedure insertImpContInfoMan (
  in_licenceplate      in imp_cont_info_man.licenceplate%type,
  in_target_state_name in imp_cont_info_man.target_state_name%type,
  in_commit            in boolean := false
);
  
procedure deleteImpContInfoMan (
  in_target_state_id in number := k_delete_all
);
  
procedure movesOsr;
  
procedure movesAsrs;
  
procedure automateMoves;

procedure deleteMoves;

procedure deleteAllocations;